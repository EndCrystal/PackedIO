package packed

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
)

// Thrown when the stream reaches the end.
var EOS = errors.New("End of stream")

// Thrown when the length of the string is greater than MAX_UINT32
var EOverflow = errors.New("Overflow")

type Input struct{ reader io.Reader }
type Output struct{ writer io.Writer }

func MakeInput(reader io.Reader) Input   { return Input{reader} }
func MakeOutput(writer io.Writer) Output { return Output{writer} }

func InputFromBuffer(buf []byte) Input {
	reader := bytes.NewReader(buf)
	return MakeInput(reader)
}

func NewOutput() (Output, *bytes.Buffer) {
	buffer := new(bytes.Buffer)
	return MakeOutput(buffer), buffer
}

type Serializable interface {
	Load(in Input)
	Save(out Output)
}

func (in Input) ReadUint8() uint8 {
	var buf [1]byte
	_, err := io.ReadFull(in.reader, buf[:])
	if err != nil {
		panic(err)
	}
	return uint8(buf[0])
}

func (in Input) ReadInt8() int8 {
	var buf [1]byte
	_, err := io.ReadFull(in.reader, buf[:])
	if err != nil {
		panic(err)
	}
	return int8(buf[0])
}

func (in Input) ReadUint16() uint16 {
	var buf [2]byte
	_, err := io.ReadFull(in.reader, buf[:])
	if err != nil {
		panic(err)
	}
	return binary.LittleEndian.Uint16(buf[:])
}

func (in Input) ReadInt16() int16 {
	var buf [2]byte
	_, err := io.ReadFull(in.reader, buf[:])
	if err != nil {
		panic(err)
	}
	return int16(binary.LittleEndian.Uint16(buf[:]))
}

func (in Input) ReadUint32() uint32 {
	var buf [4]byte
	_, err := io.ReadFull(in.reader, buf[:])
	if err != nil {
		panic(err)
	}
	return binary.LittleEndian.Uint32(buf[:])
}

func (in Input) ReadInt32() int32 {
	var buf [4]byte
	_, err := io.ReadFull(in.reader, buf[:])
	if err != nil {
		panic(err)
	}
	return int32(binary.LittleEndian.Uint32(buf[:]))
}

func (in Input) ReadUint64() uint64 {
	var buf [8]byte
	_, err := io.ReadFull(in.reader, buf[:])
	if err != nil {
		panic(err)
	}
	return binary.LittleEndian.Uint64(buf[:])
}

func (in Input) ReadInt64() int64 {
	var buf [8]byte
	_, err := io.ReadFull(in.reader, buf[:])
	if err != nil {
		panic(err)
	}
	return int64(binary.LittleEndian.Uint64(buf[:]))
}

func (in Input) ReadVarUint32() (value uint32) {
	offset := uint(0)
	for {
		var buf [1]byte
		_, err := io.ReadFull(in.reader, buf[:])
		if err != nil {
			panic(err)
		}
		ch := uint8(buf[0])
		value |= uint32(ch&0x7F) << offset
		offset += 7
		if ch&0x80 == 0 {
			return
		}
	}
}

func (in Input) ReadVarInt32() (value int32) {
	raw := in.ReadVarUint32()
	value = int32(raw >> 1)
	if raw&1 > 0 {
		value = ^value
	}
	return
}

func (in Input) ReadVarUint64() (value uint64) {
	offset := uint(0)
	for {
		var buf [1]byte
		_, err := io.ReadFull(in.reader, buf[:])
		if err != nil {
			panic(err)
		}
		ch := uint8(buf[0])
		value |= uint64(ch&0x7F) << offset
		offset += 7
		if ch&0x80 == 0 {
			return
		}
	}
}

func (in Input) ReadVarInt64() (value int64) {
	raw := in.ReadVarUint64()
	value = int64(raw >> 1)
	if raw&1 > 0 {
		value = ^value
	}
	return
}

func (in Input) ReadFloat32() float32 {
	return math.Float32frombits(in.ReadUint32())
}

func (in Input) ReadFloat64() float64 {
	return math.Float64frombits(in.ReadUint64())
}

func (in Input) ReadString() string {
	length := in.ReadVarUint32()
	buffer := make([]byte, length)
	ex, err := io.ReadFull(in.reader, buffer)
	if err != nil {
		panic(err)
	}
	if uint32(ex) != length {
		panic(EOS)
	}
	return string(buffer[:length])
}

func (in Input) ReadBytes() []byte {
	length := in.ReadVarUint32()
	buffer := make([]byte, length)
	ex, err := io.ReadFull(in.reader, buffer)
	if err != nil {
		panic(err)
	}
	if uint32(ex) != length {
		panic(EOS)
	}
	return buffer[:length]
}

func (in Input) ReadFixedBytes(buffer []byte) {
	ex, err := io.ReadFull(in.reader, buffer)
	if err != nil {
		panic(err)
	}
	if ex != len(buffer) {
		panic(EOS)
	}
}

func (in Input) IterateArray(sizefn func(length int), fn func(i int)) {
	length := in.ReadVarUint32()
	if sizefn != nil {
		sizefn(int(length))
	}
	for i := 0; i < int(length); i++ {
		fn(i)
	}
}

func (in Input) IterateObject(fn func(key string)) {
	length := in.ReadVarUint32()
	for i := 0; i < int(length); i++ {
		key := in.ReadString()
		fn(key)
	}
}

func (out Output) WriteUint8(value uint8) {
	_, err := out.writer.Write([]byte{byte(value)})
	if err != nil {
		panic(err)
	}
}

func (out Output) WriteInt8(value int8) {
	_, err := out.writer.Write([]byte{byte(value)})
	if err != nil {
		panic(err)
	}
}

func (out Output) WriteUint16(value uint16) {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], value)
	_, err := out.writer.Write(buf[:])
	if err != nil {
		panic(err)
	}
}

func (out Output) WriteInt16(value int16) {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], uint16(value))
	_, err := out.writer.Write(buf[:])
	if err != nil {
		panic(err)
	}
}

func (out Output) WriteUint32(value uint32) {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], value)
	_, err := out.writer.Write(buf[:])
	if err != nil {
		panic(err)
	}
}

func (out Output) WriteInt32(value int32) {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(value))
	_, err := out.writer.Write(buf[:])
	if err != nil {
		panic(err)
	}
}

func (out Output) WriteUint64(value uint64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], value)
	_, err := out.writer.Write(buf[:])
	if err != nil {
		panic(err)
	}
}

func (out Output) WriteInt64(value int64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(value))
	_, err := out.writer.Write(buf[:])
	if err != nil {
		panic(err)
	}
}

func (out Output) WriteVarUint32(value uint32) {
	for {
		temp := uint8(value & 0x7F)
		value >>= 7
		if value != 0 {
			temp |= 0x80
		}
		_, err := out.writer.Write([]byte{byte(temp)})
		if err != nil {
			panic(err)
		}
		if value == 0 {
			return
		}
	}
}

func (out Output) WriteVarInt32(value int32) {
	temp := uint32(value) << 1
	if value < 0 {
		temp = ^temp
	}
	out.WriteVarUint32(temp)
}

func (out Output) WriteVarUint64(value uint64) {
	for {
		temp := uint8(value & 0x7F)
		value >>= 7
		if value != 0 {
			temp |= 0x80
		}
		_, err := out.writer.Write([]byte{byte(temp)})
		if err != nil {
			panic(err)
		}
		if value == 0 {
			return
		}
	}
}

func (out Output) WriteVarInt64(value int64) {
	temp := uint64(value) << 1
	if value < 0 {
		temp = ^temp
	}
	out.WriteVarUint64(temp)
}

func (out Output) WriteFloat32(value float32) {
	temp := math.Float32bits(value)
	out.WriteVarUint32(temp)
}

func (out Output) WriteFloat64(value float64) {
	temp := math.Float64bits(value)
	out.WriteVarUint64(temp)
}

func (out Output) WriteString(value string) {
	slen := len(value)
	if uint64(slen) >= uint64(^uint32(0)) {
		panic(EOverflow)
	}
	out.WriteVarUint32(uint32(slen))
	_, err := out.writer.Write([]byte(value))
	if err != nil {
		panic(err)
	}
}

func (out Output) WriteBytes(value []byte) {
	slen := len(value)
	if uint64(slen) >= uint64(^uint32(0)) {
		panic(EOverflow)
	}
	out.WriteVarUint32(uint32(slen))
	_, err := out.writer.Write(value)
	if err != nil {
		panic(err)
	}
}

func (out Output) WriteFixedBytes(value []byte) {
	_, err := out.writer.Write(value)
	if err != nil {
		panic(err)
	}
}
