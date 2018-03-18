package maxsock

import (
	"encoding/binary"
	"fmt"
	"io"
)

var ShowLog int = 2

const FrameOffset = 4

//FrameReader impl the reader to read data as frame.
//for more info about FrameReader, see FrameReader.Read.
type FrameReader struct {
	Raw    io.Reader //the raw reader
	Offset int       //the begin position of saving frame length
}

//NewFrameReader is creator by raw reader.
func NewFrameReader(raw io.Reader, offset int) (reader *FrameReader) {
	reader = &FrameReader{
		Raw:    raw,
		Offset: offset,
	}
	return
}

//Read data as frame, 4 byte is required to save frame length on buffer p and it's begin position is FrameReader.Offset.
//so the total size of buffer is 4 byte more than the data size.
func (f *FrameReader) Read(p []byte) (n int, err error) {
	var readed, required uint32
	var once int
	buf := p
	readingBuf := buf[0 : 4+f.Offset]
	for {
		once, err = f.Raw.Read(readingBuf)
		if err != nil {
			break
		}
		readed += uint32(once)
		if required < 1 {
			if readed < 4+uint32(f.Offset) { //need more head
				readingBuf = buf[readed : 4+f.Offset]
				continue
			}
			required = binary.BigEndian.Uint32(buf[f.Offset:])
			if required > uint32(len(p)) {
				err = fmt.Errorf("bad frame size %v, buffer size is %v", required, len(p))
				break
			}
		}
		if required > readed { //need more data
			readingBuf = buf[readed:required]
			continue
		}
		n = int(readed)
		//one frame readed
		break
	}
	// fmt.Println("r-->", p[:n])
	return
}

func (f *FrameReader) Close() (err error) {
	if closer, ok := f.Raw.(io.Closer); ok {
		err = closer.Close()
	}
	return
}

func (f *FrameReader) String() string {
	return fmt.Sprintf("%p,%v,%p", f, f.Offset, f.Raw)
}

//FrameWriter impl the writer to write data as frame.
//for more info about FrameWriter, see FrameWriter.Write.
type FrameWriter struct {
	Raw    io.Writer //raw writer
	Offset int       //the begin position to save frame length
}

//NewFrameWriter is creator by raw writer.
func NewFrameWriter(raw io.Writer, offset int) (writer *FrameWriter) {
	writer = &FrameWriter{
		Raw:    raw,
		Offset: offset,
	}
	return
}

//Write data as frame, 4 byte is required to save frame length on buffer p and it's begin position is FrameWriter.Offset.
//so the total size of buffer is 4 byte more than the data size.
func (f *FrameWriter) Write(p []byte) (n int, err error) {
	binary.BigEndian.PutUint32(p, uint32(len(p)))
	n, err = f.Raw.Write(p)
	return
}

func (f *FrameWriter) Close() (err error) {
	if closer, ok := f.Raw.(io.Closer); ok {
		err = closer.Close()
	}
	return
}

func (f *FrameWriter) String() string {
	return fmt.Sprintf("%p,%v,%p", f, f.Offset, f.Raw)
}

type NoneCloserReader struct {
	io.Reader
}

func NewNoneCloserReader(reader io.Reader) *NoneCloserReader {
	return &NoneCloserReader{Reader: reader}
}

func (n *NoneCloserReader) Close() (err error) {
	return
}

func (n *NoneCloserReader) String() string {
	return fmt.Sprintf("%v", n.Reader)
}

type NoneCloserWriter struct {
	io.Writer
}

func NewNoneCloserWriter(writer io.Writer) *NoneCloserWriter {
	return &NoneCloserWriter{Writer: writer}
}

func (n *NoneCloserWriter) String() string {
	return fmt.Sprintf("%v", n.Writer)
}

func (n *NoneCloserWriter) Close() (err error) {
	return
}

type NoneCloserReadWriter struct {
	io.Writer
	io.Reader
}

func NewNoneCloserReadWriter(reader io.Reader, writer io.Writer) *NoneCloserReadWriter {
	return &NoneCloserReadWriter{Reader: reader, Writer: writer}
}

func (n *NoneCloserReadWriter) Close() (err error) {
	return
}

func (n *NoneCloserReadWriter) String() string {
	return fmt.Sprintf("%v,%v", n.Reader, n.Writer)
}

type AutoCloseReadWriter struct {
	io.Reader
	io.Writer
}

func NewAutoCloseReadWriter(reader io.Reader, writer io.Writer) *AutoCloseReadWriter {
	return &AutoCloseReadWriter{Reader: reader, Writer: writer}
}

func (a *AutoCloseReadWriter) Close() (err error) {
	if closer, ok := a.Reader.(io.Closer); ok {
		xerr := closer.Close()
		if xerr != nil {
			err = xerr
		}
	}
	if closer, ok := a.Writer.(io.Closer); ok {
		xerr := closer.Close()
		if xerr != nil {
			err = xerr
		}
	}
	return
}

func (a *AutoCloseReadWriter) String() string {
	return fmt.Sprintf("%v,%v", a.Reader, a.Writer)
}
