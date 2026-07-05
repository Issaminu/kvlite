package internal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
)

// EncodingOptions defines how to encode/decode structs
type EncodingOptions struct {
	ByteOrder binary.ByteOrder // binary.LittleEndian or binary.BigEndian
}

// DefaultEncodingOptions provides sensible defaults for KVLite
func DefaultEncodingOptions() *EncodingOptions {
	return &EncodingOptions{
		ByteOrder: binary.LittleEndian,
	}
}

// StructToBytes converts any struct to a byte slice
func StructToBytes(v interface{}, opts *EncodingOptions) ([]byte, error) {
	if opts == nil {
		opts = DefaultEncodingOptions()
	}

	buf := new(bytes.Buffer)
	val := reflect.ValueOf(v)

	// Handle pointer to struct
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected struct, got %v", val.Kind())
	}

	if err := encodeStruct(buf, val, opts); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// BytesToStruct decodes a byte slice into a struct
func BytesToStruct(data []byte, v interface{}, opts *EncodingOptions) error {
	if opts == nil {
		opts = DefaultEncodingOptions()
	}

	buf := bytes.NewReader(data)
	val := reflect.ValueOf(v)

	// Must be a pointer to a struct
	if val.Kind() != reflect.Ptr {
		return fmt.Errorf("expected pointer to struct, got %v", val.Kind())
	}

	val = val.Elem()
	if val.Kind() != reflect.Struct {
		return fmt.Errorf("expected pointer to struct, got pointer to %v", val.Kind())
	}

	return decodeStruct(buf, val, opts)
}

func encodeStruct(buf *bytes.Buffer, val reflect.Value, opts *EncodingOptions) error {
	typ := val.Type()

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		fieldType := typ.Field(i)

		// Skip unexported fields
		if !field.CanInterface() {
			continue
		}

		// Check for struct tags (e.g., `kv:"skip"`)
		tag := fieldType.Tag.Get("kv")
		if tag == "skip" || tag == "-" {
			continue
		}

		if err := encodeField(buf, field, opts); err != nil {
			return fmt.Errorf("field %s: %w", fieldType.Name, err)
		}
	}

	return nil
}

func encodeField(buf *bytes.Buffer, field reflect.Value, opts *EncodingOptions) error {
	switch field.Kind() {
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return binary.Write(buf, opts.ByteOrder, field.Interface())

	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return binary.Write(buf, opts.ByteOrder, field.Interface())

	case reflect.Float32, reflect.Float64:
		return binary.Write(buf, opts.ByteOrder, field.Interface())

	case reflect.Bool:
		var b uint8
		if field.Bool() {
			b = 1
		}
		return binary.Write(buf, opts.ByteOrder, b)

	case reflect.String:
		return encodeString(buf, field.String(), opts)

	case reflect.Slice:
		return encodeSlice(buf, field, opts)

	case reflect.Array:
		return encodeArray(buf, field, opts)

	case reflect.Struct:
		return encodeStruct(buf, field, opts)

	default:
		return fmt.Errorf("unsupported type: %v", field.Kind())
	}
}

func encodeString(buf *bytes.Buffer, s string, opts *EncodingOptions) error {
	strBytes := []byte(s)
	length := uint32(len(strBytes))

	// Write length prefix
	if err := binary.Write(buf, opts.ByteOrder, length); err != nil {
		return err
	}

	// Write string bytes
	_, err := buf.Write(strBytes)
	return err
}

func encodeSlice(buf *bytes.Buffer, field reflect.Value, opts *EncodingOptions) error {
	// Write length prefix
	length := uint32(field.Len())
	if err := binary.Write(buf, opts.ByteOrder, length); err != nil {
		return err
	}

	// Write elements
	for i := 0; i < field.Len(); i++ {
		elem := field.Index(i)
		if err := encodeField(buf, elem, opts); err != nil {
			return err
		}
	}

	return nil
}

func encodeArray(buf *bytes.Buffer, field reflect.Value, opts *EncodingOptions) error {
	// Arrays have fixed size, no length prefix needed
	for i := 0; i < field.Len(); i++ {
		elem := field.Index(i)

		// For byte arrays, write directly
		if elem.Kind() == reflect.Uint8 {
			if err := buf.WriteByte(byte(elem.Uint())); err != nil {
				return err
			}
		} else {
			if err := encodeField(buf, elem, opts); err != nil {
				return err
			}
		}
	}
	return nil
}

// Decoding functions

func decodeStruct(buf *bytes.Reader, val reflect.Value, opts *EncodingOptions) error {
	typ := val.Type()

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		fieldType := typ.Field(i)

		// Skip unexported fields
		if !field.CanSet() {
			continue
		}

		// Check for struct tags
		tag := fieldType.Tag.Get("kv")
		if tag == "skip" || tag == "-" {
			continue
		}

		if err := decodeField(buf, field, opts); err != nil {
			return fmt.Errorf("field %s: %w", fieldType.Name, err)
		}
	}

	return nil
}

func decodeField(buf *bytes.Reader, field reflect.Value, opts *EncodingOptions) error {
	switch field.Kind() {
	case reflect.Int8:
		var v int8
		if err := binary.Read(buf, opts.ByteOrder, &v); err != nil {
			return err
		}
		field.SetInt(int64(v))

	case reflect.Int16:
		var v int16
		if err := binary.Read(buf, opts.ByteOrder, &v); err != nil {
			return err
		}
		field.SetInt(int64(v))

	case reflect.Int32:
		var v int32
		if err := binary.Read(buf, opts.ByteOrder, &v); err != nil {
			return err
		}
		field.SetInt(int64(v))

	case reflect.Int64:
		var v int64
		if err := binary.Read(buf, opts.ByteOrder, &v); err != nil {
			return err
		}
		field.SetInt(v)

	case reflect.Uint8:
		var v uint8
		if err := binary.Read(buf, opts.ByteOrder, &v); err != nil {
			return err
		}
		field.SetUint(uint64(v))

	case reflect.Uint16:
		var v uint16
		if err := binary.Read(buf, opts.ByteOrder, &v); err != nil {
			return err
		}
		field.SetUint(uint64(v))

	case reflect.Uint32:
		var v uint32
		if err := binary.Read(buf, opts.ByteOrder, &v); err != nil {
			return err
		}
		field.SetUint(uint64(v))

	case reflect.Uint64:
		var v uint64
		if err := binary.Read(buf, opts.ByteOrder, &v); err != nil {
			return err
		}
		field.SetUint(v)

	case reflect.Float32:
		var v float32
		if err := binary.Read(buf, opts.ByteOrder, &v); err != nil {
			return err
		}
		field.SetFloat(float64(v))

	case reflect.Float64:
		var v float64
		if err := binary.Read(buf, opts.ByteOrder, &v); err != nil {
			return err
		}
		field.SetFloat(v)

	case reflect.Bool:
		var v uint8
		if err := binary.Read(buf, opts.ByteOrder, &v); err != nil {
			return err
		}
		field.SetBool(v != 0)

	case reflect.String:
		return decodeString(buf, field, opts)

	case reflect.Slice:
		return decodeSlice(buf, field, opts)

	case reflect.Array:
		return decodeArray(buf, field, opts)

	case reflect.Struct:
		return decodeStruct(buf, field, opts)

	default:
		return fmt.Errorf("unsupported type: %v", field.Kind())
	}

	return nil
}

func decodeString(buf *bytes.Reader, field reflect.Value, opts *EncodingOptions) error {
	// Read length prefix
	var length uint32
	if err := binary.Read(buf, opts.ByteOrder, &length); err != nil {
		return err
	}

	// Read string bytes
	strBytes := make([]byte, length)
	if _, err := buf.Read(strBytes); err != nil {
		return err
	}

	field.SetString(string(strBytes))
	return nil
}

func decodeSlice(buf *bytes.Reader, field reflect.Value, opts *EncodingOptions) error {
	// Read length prefix
	var length uint32
	if err := binary.Read(buf, opts.ByteOrder, &length); err != nil {
		return err
	}

	// Create slice with appropriate capacity
	slice := reflect.MakeSlice(field.Type(), int(length), int(length))

	// Read elements
	for i := 0; i < int(length); i++ {
		elem := slice.Index(i)
		if err := decodeField(buf, elem, opts); err != nil {
			return err
		}
	}

	field.Set(slice)
	return nil
}

func decodeArray(buf *bytes.Reader, field reflect.Value, opts *EncodingOptions) error {
	// Arrays have fixed size
	for i := 0; i < field.Len(); i++ {
		elem := field.Index(i)

		// For byte arrays, read directly
		if elem.Kind() == reflect.Uint8 {
			b, err := buf.ReadByte()
			if err != nil {
				return err
			}
			elem.SetUint(uint64(b))
		} else {
			if err := decodeField(buf, elem, opts); err != nil {
				return err
			}
		}
	}
	return nil
}
