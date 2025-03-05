//  Copyright (c) 2023 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scorch

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring/v2"
	index "github.com/blevesearch/bleve_index_api"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

// FileSegment implements segment.Segment using a file-based approach without memory mapping
type FileSegment struct {
	path string
	file *os.File
	size int64

	refs int64
	m    sync.Mutex

	// Track bytes read for stats reporting
	bytesRead uint64

	// Segment metadata
	header       *segmentHeader
	fieldsMap    map[string]uint16 // fieldName -> fieldID+1
	fieldsInv    []string          // fieldID -> fieldName
	docValueData map[string][]byte // field -> docValue data
}

// segmentHeader represents the metadata at the start of a segment file
type segmentHeader struct {
	// Header fields would mirror the structure of the segment file format
	// For simplicity we'll handle the bare minimum needed
	Version              uint32
	FieldsIndexPos       uint64
	StoredIndexPos       uint64
	DocValuePos          uint64
	DocCount             uint64
	NumDocs              uint64
	StoredFieldsIndexPos uint64
}

// FileDocValueReader provides access to doc values
type FileDocValueReader struct {
}

// NewFileSegment creates a new file-based segment
func NewFileSegment(path string) (*FileSegment, error) {
	// Open the file
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	fs := &FileSegment{
		path:         path,
		file:         file,
		size:         fileInfo.Size(),
		refs:         1,
		fieldsMap:    make(map[string]uint16),
		docValueData: make(map[string][]byte),
	}

	// Read and parse the header
	err = fs.readHeader()
	if err != nil {
		_ = fs.Close()
		return nil, err
	}

	// Read field definitions
	err = fs.readFields()
	if err != nil {
		_ = fs.Close()
		return nil, err
	}

	return fs, nil
}

// readHeader reads and parses the segment header
func (fs *FileSegment) readHeader() error {
	// Read the first 4K or so bytes which should contain header information
	headerBuf := make([]byte, 4096)
	_, err := fs.file.ReadAt(headerBuf, 0)
	if err != nil && err != io.EOF {
		return err
	}

	// Parse header information
	// This is a simplified example; actual implementation would depend on the segment format
	fs.header = &segmentHeader{
		Version:              binary.LittleEndian.Uint32(headerBuf[0:4]),
		FieldsIndexPos:       binary.LittleEndian.Uint64(headerBuf[8:16]),
		StoredIndexPos:       binary.LittleEndian.Uint64(headerBuf[16:24]),
		DocValuePos:          binary.LittleEndian.Uint64(headerBuf[24:32]),
		DocCount:             binary.LittleEndian.Uint64(headerBuf[32:40]),
		NumDocs:              binary.LittleEndian.Uint64(headerBuf[40:48]),
		StoredFieldsIndexPos: binary.LittleEndian.Uint64(headerBuf[48:56]),
	}

	atomic.AddUint64(&fs.bytesRead, 4096)
	return nil
}

// readFields reads the field definitions from the segment
func (fs *FileSegment) readFields() error {
	// Read field definitions from the file
	// This is a simplified implementation; actual implementation would depend on the segment format
	if fs.header == nil || fs.header.FieldsIndexPos == 0 {
		return fmt.Errorf("invalid segment header or fields position")
	}

	// Read the fields section
	fieldBuf := make([]byte, 4096) // Adjust based on expected size
	_, err := fs.file.ReadAt(fieldBuf, int64(fs.header.FieldsIndexPos))
	if err != nil && err != io.EOF {
		return err
	}

	// Parse field data
	// For now, just a placeholder implementation
	// In a real implementation, this would parse the actual field data format
	numFields := binary.LittleEndian.Uint16(fieldBuf[0:2])
	fs.fieldsInv = make([]string, numFields)

	pos := 2
	for i := uint16(0); i < numFields; i++ {
		fieldLen := int(binary.LittleEndian.Uint16(fieldBuf[pos : pos+2]))
		pos += 2
		fieldName := string(fieldBuf[pos : pos+fieldLen])
		pos += fieldLen

		fs.fieldsMap[fieldName] = i + 1
		fs.fieldsInv[i] = fieldName
	}

	atomic.AddUint64(&fs.bytesRead, 4096)
	return nil
}

// Read reads from the file segment
func (fs *FileSegment) Read(offset int64, length int) ([]byte, error) {
	buf := make([]byte, length)

	_, err := fs.file.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return nil, err
	}

	atomic.AddUint64(&fs.bytesRead, uint64(length))
	return buf, nil
}

// Close implements segment.Segment interface Close method
func (fs *FileSegment) Close() error {
	return fs.DecRef()
}

// DecRef decrements the reference count and closes the file when the count reaches 0
func (fs *FileSegment) DecRef() error {
	fs.m.Lock()
	defer fs.m.Unlock()

	fs.refs--
	if fs.refs == 0 {
		// Close the file
		return fs.file.Close()
	}

	return nil
}

// AddRef increments the reference count
func (fs *FileSegment) AddRef() {
	fs.m.Lock()
	defer fs.m.Unlock()
	fs.refs++
}

// Dictionary returns a term dictionary for the specified field
func (fs *FileSegment) Dictionary(field string) (segment.TermDictionary, error) {
	fieldID, exists := fs.fieldsMap[field]
	if !exists {
		return nil, fmt.Errorf("field %s does not exist", field)
	}

	// We'd need to read dictionary location from the segment file
	// This is a simplified example
	dictionaryPos := fs.header.FieldsIndexPos + 8 + uint64(fieldID-1)*16

	// Read the dictionary metadata
	buf := make([]byte, 16)
	_, err := fs.file.ReadAt(buf, int64(dictionaryPos))
	if err != nil && err != io.EOF {
		return nil, err
	}
	atomic.AddUint64(&fs.bytesRead, 16)

	termsPos := binary.LittleEndian.Uint64(buf[0:8])
	termsCount := binary.LittleEndian.Uint64(buf[8:16])

	return &FileTermDictionary{
		segment:       fs,
		field:         field,
		fieldID:       fieldID,
		dictionaryPos: termsPos,
		termsCount:    termsCount,
	}, nil
}

// VisitStoredFields calls the given function for each stored field
func (fs *FileSegment) VisitStoredFields(documentNumber uint64, visitor segment.StoredFieldValueVisitor) error {
	// Find the stored field section for this document
	if documentNumber >= fs.Count() {
		return fmt.Errorf("document number %d out of range for segment with %d docs", documentNumber, fs.Count())
	}

	// Read the stored field section offset
	offsetBuf := make([]byte, 8)
	offsetPos := fs.header.StoredFieldsIndexPos + documentNumber*8
	_, err := fs.file.ReadAt(offsetBuf, int64(offsetPos))
	if err != nil && err != io.EOF {
		return err
	}
	atomic.AddUint64(&fs.bytesRead, 8)

	// Read the stored field section
	// In a real implementation, we'd read the stored fields from the file
	// and properly decode their values

	// We'll just invoke the visitor with dummy data
	keepGoing := visitor("_id", 'x', []byte(fmt.Sprintf("doc%d", documentNumber)), nil)
	if !keepGoing {
		return nil
	}

	visitor("text", 't', []byte(fmt.Sprintf("content for document %d", documentNumber)), nil)
	return nil
}

// DocID returns the identifier for the specified document
func (fs *FileSegment) DocID(num uint64) ([]byte, error) {
	if num >= fs.Count() {
		return nil, fmt.Errorf("document number out of range")
	}

	// Implementation would read document ID from the file
	// This is a complex operation that depends on the segment format
	return nil, fmt.Errorf("DocID not implemented for file-based segment")
}

// Count returns the number of documents in this segment
func (fs *FileSegment) Count() uint64 {
	return fs.header.NumDocs
}

// DocNumbers returns document numbers for the provided identifiers
func (fs *FileSegment) DocNumbers(docIDs []string) (*roaring.Bitmap, error) {
	// In a real implementation, we'd look up the doc IDs in the index
	// For now, return an empty bitmap
	return roaring.New(), nil
}

// Fields returns the names of all fields in this segment
func (fs *FileSegment) Fields() []string {
	result := make([]string, len(fs.fieldsInv))
	copy(result, fs.fieldsInv)
	return result
}

// Size returns the size of the segment in memory
func (fs *FileSegment) Size() int {
	// Approximate size of the FileSegment struct and its fields
	size := 200 // Base struct size

	// Add size of maps and slices
	size += len(fs.fieldsMap) * 30     // Rough estimate for fieldsMap
	size += len(fs.fieldsInv) * 20     // Rough estimate for fieldsInv
	size += len(fs.docValueData) * 100 // Rough estimate for docValueData

	return size
}

// BytesRead returns the number of bytes read from this segment
func (fs *FileSegment) BytesRead() uint64 {
	return atomic.LoadUint64(&fs.bytesRead)
}

// ResetBytesRead resets the bytesRead counter to the provided value
func (fs *FileSegment) ResetBytesRead(newValue uint64) {
	atomic.StoreUint64(&fs.bytesRead, newValue)
}

// Path returns the file path for this segment
func (fs *FileSegment) Path() string {
	return fs.path
}

// BytesWritten returns 0 as a file segment doesn't write bytes
func (fs *FileSegment) BytesWritten() uint64 {
	return 0
}

// Add these new types for dictionary operations

// FileTermDictionary implements the segment.TermDictionary interface
type FileTermDictionary struct {
	segment       *FileSegment
	field         string
	fieldID       uint16
	dictionaryPos uint64
	termsCount    uint64
}

// FilePostingsList implements the segment.PostingsList interface
type FilePostingsList struct {
	segment     *FileSegment
	field       string
	term        []byte
	postingsPos uint64
	count       uint64
}

// FilePostingsIterator implements segment.PostingsIterator
type FilePostingsIterator struct {
	postingsList *FilePostingsList
	curr         segment.Posting
	docNum       uint64
	iterStarted  bool
}

// FilePosting implements segment.Posting
type FilePosting struct {
	docNum    uint64
	freq      uint64
	norm      float64
	locations []segment.Location
}

// PostingsList returns a postings list for the specified term
func (ftd *FileTermDictionary) PostingsList(term []byte, except *roaring.Bitmap, prealloc segment.PostingsList) (segment.PostingsList, error) {
	// In a real implementation, we'd:
	// 1. Find the term in the dictionary
	// 2. Get its postings list location
	// 3. Return a FilePostingsList pointing to that location

	// Simplified implementation
	postingsPos := ftd.dictionaryPos + 8 + uint64(len(term))

	return &FilePostingsList{
		segment:     ftd.segment,
		field:       ftd.field,
		term:        term,
		postingsPos: postingsPos,
		count:       0, // We'd read this from the file in a real implementation
	}, nil
}

// FileDictionaryIterator implements segment.DictionaryIterator
type FileDictionaryIterator struct {
	dict        *FileTermDictionary
	currEntry   *index.DictEntry
	iterStarted bool
}

// Next returns the next term in the dictionary
func (fdi *FileDictionaryIterator) Next() (*index.DictEntry, error) {
	// In a real implementation, we'd read the next term from the file
	// For this simplified implementation, we'll return empty right away
	if !fdi.iterStarted {
		fdi.iterStarted = true
		// Start reading from the first item
	} else {
		// Move to next item
	}

	// No more entries
	return nil, nil
}

// Current returns the current term in the dictionary
func (fdi *FileDictionaryIterator) Current() *index.DictEntry {
	return fdi.currEntry
}

// BytesRead returns the number of bytes read from the dictionary
func (fdi *FileDictionaryIterator) BytesRead() uint64 {
	// In a real implementation, we would track bytes read
	return 0
}

// BytesWritten returns the number of bytes written by this iterator
func (fdi *FileDictionaryIterator) BytesWritten() uint64 {
	// File-based implementation doesn't write any bytes
	return 0
}

// ResetBytesRead resets the bytes read counter
func (fdi *FileDictionaryIterator) ResetBytesRead(counter uint64) {
	// Nothing to do for file-based implementation
}

// AutomatonIterator for FileTermDictionary
func (ftd *FileTermDictionary) AutomatonIterator(a segment.Automaton, startKeyInclusive, endKeyExclusive []byte) segment.DictionaryIterator {
	return &FileDictionaryIterator{
		dict:        ftd,
		currEntry:   nil,
		iterStarted: false,
	}
}

// Contains checks if the term exists in the dictionary
func (ftd *FileTermDictionary) Contains(key []byte) (bool, error) {
	// A real implementation would search for the term in the dictionary
	// For simplicity, we return a placeholder result
	return false, nil
}

// Iterator returns a PostingsIterator for this postings list
func (fpl *FilePostingsList) Iterator(includeFreq, includeNorm, includeLocations bool, prealloc segment.PostingsIterator) segment.PostingsIterator {
	return &FilePostingsIterator{
		postingsList: fpl,
		docNum:       0,
		iterStarted:  false,
	}
}

// Size returns the memory size of this postings list
func (fpl *FilePostingsList) Size() int {
	return 100 // Simplified approximation
}

// Count returns the number of postings in this list
func (fpl *FilePostingsList) Count() uint64 {
	return fpl.count
}

// Next moves to the next posting
func (fpi *FilePostingsIterator) Next() (segment.Posting, error) {
	if !fpi.iterStarted {
		fpi.iterStarted = true
		// Ideally, we'd read the first posting from the file
	} else {
		// Advance to next posting
		fpi.docNum++
	}

	// Check if we've gone past the end
	if fpi.docNum >= fpi.postingsList.count {
		return nil, nil
	}

	// In a real implementation, we'd read posting data from the file
	// For simplicity, we return a placeholder posting
	fpi.curr = &FilePosting{
		docNum: fpi.docNum,
		freq:   1,
		norm:   1.0,
	}

	return fpi.curr, nil
}

// Advance moves to the posting with the specified document number
func (fpi *FilePostingsIterator) Advance(docNum uint64) (segment.Posting, error) {
	if fpi.docNum >= docNum {
		return fpi.Next()
	}

	fpi.docNum = docNum - 1
	return fpi.Next()
}

// Size returns the memory size of this iterator
func (fpi *FilePostingsIterator) Size() int {
	return 100 // Simplified approximation
}

// Number returns the document number of this posting
func (fp *FilePosting) Number() uint64 {
	return fp.docNum
}

// Frequency returns the frequency of this posting
func (fp *FilePosting) Frequency() uint64 {
	return fp.freq
}

// Norm returns the norm of this posting
func (fp *FilePosting) Norm() float64 {
	return fp.norm
}

// Locations returns the locations of this posting
func (fp *FilePosting) Locations() []segment.Location {
	return fp.locations
}

// Size returns the memory size of this posting
func (fp *FilePosting) Size() int {
	return 100 // Simplified approximation
}

// BytesRead returns the number of bytes read by this postings list
func (fpl *FilePostingsList) BytesRead() uint64 {
	// In a real implementation, we would track bytes read
	return 0
}

// BytesRead returns the number of bytes read by this iterator
func (fpi *FilePostingsIterator) BytesRead() uint64 {
	// In a real implementation, we would track bytes read
	return 0
}

// Cardinality returns the number of terms in the dictionary
func (ftd *FileTermDictionary) Cardinality() int {
	return int(ftd.termsCount)
}

// BytesWritten returns the number of bytes written by this postings list
func (fpl *FilePostingsList) BytesWritten() uint64 {
	// File-based implementation doesn't write any bytes
	return 0
}

// BytesWritten returns the number of bytes written by this iterator
func (fpi *FilePostingsIterator) BytesWritten() uint64 {
	// File-based implementation doesn't write any bytes
	return 0
}

// ResetBytesRead resets the bytes read counter
func (fpl *FilePostingsList) ResetBytesRead(counter uint64) {
	// Nothing to do for file-based implementation
}

// ResetBytesRead resets the bytes read counter
func (fpi *FilePostingsIterator) ResetBytesRead(counter uint64) {
	// Nothing to do for file-based implementation
}
