package packed

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"

	"errors"
)

// Thrown when the stream reaches the end.
var EOS = errors.New("End of stream")

// Thrown when the length of the string is greater than MAX_UINT32
var EOverflow = errors.New("Overflow")

type PackedInput struct{ reader io.Reader }
type PackedOutput struct{ writer io.Writer }

func MakeInput(reader io.Reader) PackedInput   { return PackedInput{reader} }
func MakeOutput(writer io.Writer) PackedOutput { return PackedOutput{writer} }

func InputFromBuffer(buf []byte) PackedInput {
	reader := bytes.NewReader(buf)
	return MakeInput(reader)
}

func NewOutput() (PackedOutput, *bytes.Buffer) {
	buffer := new(bytes.Buffer)
	return MakeOutput(buffer), buffer
}

type PackedSerializable interface {
	Load(in PackedInput)
	Save(out PackedOutput)
}

func (in PackedInput) ReadUint8() uint8 {
	var buf [1]byte
	_, err := io.ReadFull(in.reader, buf[:])
	if err != nil {
		panic(err)
	}
	return uint8(buf[0])
}

func (in PackedInput) ReadInt8() int8 {
	var buf [1]byte
	_, err := io.ReadFull(in.reader, buf[:])
	if err != nil {
		panic(err)
	}
	return int8(buf[0])
}

func (in PackedInput) ReadUint16() uint16 {
	var buf [2]byte
	_, err := io.ReadFull(in.reader, buf[:])
	if err != nil {
		panic(err)
	}
	return binary.LittleEndian.Uint16(buf[:])
}

func (in PackedInput) ReadInt16() int16 {
	var buf [2]byte
	_, err := io.ReadFull(in.reader, buf[:])
	if err != nil {
		panic(err)
	}
	return int16(binary.LittleEndian.Uint16(buf[:]))
}

func (in PackedInput) ReadUint32() uint32 {
	var buf [4]byte
	_, err := io.ReadFull(in.reader, buf[:])
	if err != nil {
		panic(err)
	}
	return binary.LittleEndian.Uint32(buf[:])
}

func (in PackedInput) ReadInt32() int32 {
	var buf [4]byte
	_, err := io.ReadFull(in.reader, buf[:])
	if err != nil {
		panic(err)
	}
	return int32(binary.LittleEndian.Uint32(buf[:]))
}

func (in PackedInput) ReadUint64() uint64 {
	var buf [8]byte
	_, err := io.ReadFull(in.reader, buf[:])
	if err != nil {
		panic(err)
	}
	return binary.LittleEndian.Uint64(buf[:])
}

func (in PackedInput) ReadInt64() int64 {
	var buf [8]byte
	_, err := io.ReadFull(in.reader, buf[:])
	if err != nil {
		panic(err)
	}
	return int64(binary.LittleEndian.Uint64(buf[:]))
}

func (in PackedInput) ReadVarUint32() (value uint32) {
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

func (in PackedInput) ReadVarInt32() (value int32) {
	raw := in.ReadVarUint32()
	value = int32(raw >> 1)
	if raw&1 > 0 {
		value = ^value
	}
	return
}

func (in PackedInput) ReadVarUint64() (value uint64) {
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

func (in PackedInput) ReadVarInt64() (value int64) {
	raw := in.ReadVarUint64()
	value = int64(raw >> 1)
	if raw&1 > 0 {
		value = ^value
	}
	return
}

func (in PackedInput) ReadFloat32() float32 {
	return math.Float32frombits(in.ReadUint32())
}

func (in PackedInput) ReadFloat64() float64 {
	return math.Float64frombits(in.ReadUint64())
}

func (in PackedInput) ReadString() string {
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

func (in PackedInput) IterateArray(sizefn func(length int), fn func(i int, in PackedInput)) {
	length := in.ReadVarUint32()
	if sizefn != nil {
		sizefn(int(length))
	}
	for i := 0; i < int(length); i++ {
		fn(i, in)
	}
}

func (in PackedInput) IterateObject(fn func(i int, key string, in PackedInput)) {
	length := in.ReadVarUint32()
	for i := 0; i < int(length); i++ {
		key := in.ReadString()
		fn(i, key, in)
	}
}

func (out PackedOutput) WriteUint8(value uint8) {
	_, err := out.writer.Write([]byte{byte(value)})
	if err != nil {
		panic(err)
	}
}

func (out PackedOutput) WriteInt8(value int8) {
	_, err := out.writer.Write([]byte{byte(value)})
	if err != nil {
		panic(err)
	}
}

func (out PackedOutput) WriteUint16(value uint16) {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], value)
	_, err := out.writer.Write(buf[:])
	if err != nil {
		panic(err)
	}
}

func (out PackedOutput) WriteInt16(value int16) {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], uint16(value))
	_, err := out.writer.Write(buf[:])
	if err != nil {
		panic(err)
	}
}

func (out PackedOutput) WriteUint32(value uint32) {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], value)
	_, err := out.writer.Write(buf[:])
	if err != nil {
		panic(err)
	}
}

func (out PackedOutput) WriteInt32(value int32) {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(value))
	_, err := out.writer.Write(buf[:])
	if err != nil {
		panic(err)
	}
}

func (out PackedOutput) WriteUint64(value uint64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], value)
	_, err := out.writer.Write(buf[:])
	if err != nil {
		panic(err)
	}
}

func (out PackedOutput) WriteInt64(value int64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(value))
	_, err := out.writer.Write(buf[:])
	if err != nil {
		panic(err)
	}
}

func (out PackedOutput) WriteVarUint32(value uint32) {
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

func (out PackedOutput) WriteVarInt32(value int32) {
	temp := uint32(value) << 1
	if value < 0 {
		temp = ^temp
	}
	out.WriteVarUint32(temp)
}

func (out PackedOutput) WriteVarUint64(value uint64) {
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

func (out PackedOutput) WriteVarInt64(value int64) {
	temp := uint64(value) << 1
	if value < 0 {
		temp = ^temp
	}
	out.WriteVarUint64(temp)
}

func (out PackedOutput) WriteFloat32(value float32) {
	temp := math.Float32bits(value)
	out.WriteVarUint32(temp)
}

func (out PackedOutput) WriteFloat64(value float64) {
	temp := math.Float64bits(value)
	out.WriteVarUint64(temp)
}

func (out PackedOutput) WriteString(value string) {
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

func (out PackedOutput) WriteAnyArray(num uint32, cb func(i uint32, out PackedOutput)) {
	out.WriteVarUint32(num)
	for i := uint32(0); i < num; i++ {
		cb(i, out)
	}
}

func (out PackedOutput) WriteArray(arr []PackedSerializable) {
	slen := len(arr)
	if uint64(slen) >= uint64(^uint32(0)) {
		panic(EOverflow)
	}
	out.WriteVarUint32(uint32(slen))
	for _, item := range arr {
		item.Save(out)
	}
}
