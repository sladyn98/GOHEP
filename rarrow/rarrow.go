package rarrow

import (
	"fmt"
	"log"
	"reflect"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"go-hep.org/x/hep/groot/rtree"
)

// CreateArrowSchema creates a custom arrow schema from a rtree.Tree object
func CreateArrowSchema(tree rtree.Tree) *arrow.Schema {

	var arrowFields = []arrow.Field{}
	var arrowObj = new(arrow.Field)
	for _, branches := range tree.Branches() {
		//TODO: select branches based on gotype
		fmt.Println(branches.Name())

		switch branchName := branches.Name(); branchName {

		case "Int8":
			arrowObj.Name = "fi-i8"
			arrowObj.Type = arrow.PrimitiveTypes.Int8
			arrowFields = append(arrowFields, *arrowObj)

		case "Int16":
			arrowObj.Name = "fi-i16"
			arrowObj.Type = arrow.PrimitiveTypes.Int16
			arrowFields = append(arrowFields, *arrowObj)

		case "Int32":
			arrowObj.Name = "fi-i32"
			arrowObj.Type = arrow.PrimitiveTypes.Int32
			arrowFields = append(arrowFields, *arrowObj)

		case "Int64":
			arrowObj.Name = "fi-i64"
			arrowObj.Type = arrow.PrimitiveTypes.Int64
			arrowFields = append(arrowFields, *arrowObj)

		case "Uint8":
			arrowObj.Name = "fi-Ui8"
			arrowObj.Type = arrow.PrimitiveTypes.Uint8
			arrowFields = append(arrowFields, *arrowObj)

		case "Uint16":
			arrowObj.Name = "fi-Ui16"
			arrowObj.Type = arrow.PrimitiveTypes.Uint16
			arrowFields = append(arrowFields, *arrowObj)

		case "UInt32":
			arrowObj.Name = "fi-Ui32"
			arrowObj.Type = arrow.PrimitiveTypes.Uint32
			arrowFields = append(arrowFields, *arrowObj)

		case "UInt64":
			arrowObj.Name = "fi-Ui64"
			arrowObj.Type = arrow.PrimitiveTypes.Uint64
			arrowFields = append(arrowFields, *arrowObj)

		case "Float32":
			arrowObj.Name = "fi-F8"
			arrowObj.Type = arrow.PrimitiveTypes.Float32
			arrowFields = append(arrowFields, *arrowObj)

		case "Float64":
			arrowObj.Name = "fi-F64"
			arrowObj.Type = arrow.PrimitiveTypes.Float64
			arrowFields = append(arrowFields, *arrowObj)

		case "Date32":
			arrowObj.Name = "fi-D32"
			arrowObj.Type = arrow.PrimitiveTypes.Date32
			arrowFields = append(arrowFields, *arrowObj)

		case "Date64":
			arrowObj.Name = "fi-D64"
			arrowObj.Type = arrow.PrimitiveTypes.Date64
			arrowFields = append(arrowFields, *arrowObj)

		case "Str":
			arrowObj.Name = "fi-str"
			arrowObj.Type = arrow.BinaryTypes.String
			arrowFields = append(arrowFields, *arrowObj)

		default:
			fmt.Println("Sorry unmatchable type")
		}

	}

	arrowSchema := arrow.NewSchema(arrowFields, nil)
	return arrowSchema

}

// CreateTableReader creates a table reader from the arrow schema and rtree.tree object
func CreateTableReader(arrowSchema *arrow.Schema, tree rtree.Tree) *array.TableReader {

	// Add the tree entries to n tuples
	var nt = ntuple{n: tree.Entries()}
	for _, leaf := range tree.Leaves() {
		if leaf.Kind() == reflect.String {
			nt.add(leaf.Name(), leaf)
			continue
		}
		nt.add(leaf.Name(), leaf)
	}

	//Create arrays to append to the Record

	var i8array []int8
	var i16array []int16
	var i32array []int32
	var i64array []int64
	var ui8array []uint8
	var ui16array []uint16
	var ui32array []uint32
	var ui64array []uint64
	var f32array []float32
	var f64array []float64
	var str []string

	//Scan the n tuples
	sc, err := rtree.NewTreeScannerVars(tree, nt.args...)
	if err != nil {
		log.Fatal(err)
	}
	defer sc.Close()
	nrows := 0
	for sc.Next() {
		err = sc.Scan(nt.vars...)
		if err != nil {
			log.Fatal(err)
		}

		for i := range nt.cols {
			col := &nt.cols[i]
			fmt.Println("look at the data", col.name)

			switch colDataType := col.name; colDataType {

			case "Int8":
				i8array = append(i8array, col.data.Interface().(int8))

			case "Int16":
				i16array = append(i16array, col.data.Interface().(int16))

			case "Int32":
				i32array = append(i32array, col.data.Interface().(int32))

			case "Int64":
				i64array = append(i64array, col.data.Interface().(int64))

			case "UInt8":
				ui8array = append(ui8array, col.data.Interface().(uint8))

			case "UInt16":
				ui16array = append(ui16array, col.data.Interface().(uint16))

			case "UInt32":
				ui32array = append(ui32array, uint32(col.data.Interface().(int32)))

			case "UInt64":
				ui64array = append(ui64array, uint64(col.data.Interface().(int64)))

			case "Float32":
				f32array = append(f32array, col.data.Interface().(float32))

			case "Float64":
				f64array = append(f64array, col.data.Interface().(float64))

			case "Str":
				str = append(str, col.data.Interface().(string))

			}

		}
		nrows++
	}

	// Create memory pool and record builder
	pool := memory.NewGoAllocator()
	recordBuilder := array.NewRecordBuilder(pool, arrowSchema)

	// Check for empty arrays and append and return record builder.

	index := 0

	if len(i8array) != 0 {
		recordBuilder.Field(index).(*array.Int8Builder).AppendValues(i8array, nil)
		index++
	}
	if len(i16array) != 0 {
		recordBuilder.Field(index).(*array.Int16Builder).AppendValues(i16array, nil)
		index++
	}
	if len(i32array) != 0 {
		recordBuilder.Field(index).(*array.Int32Builder).AppendValues(i32array, nil)
		index++
	}
	if len(i64array) != 0 {
		recordBuilder.Field(index).(*array.Int64Builder).AppendValues(i64array, nil)
		index++
	}
	if len(ui8array) != 0 {
		recordBuilder.Field(index).(*array.Uint8Builder).AppendValues(ui8array, nil)
		index++
	}
	if len(ui16array) != 0 {
		recordBuilder.Field(index).(*array.Uint16Builder).AppendValues(ui16array, nil)
		index++
	}
	if len(ui32array) != 0 {
		recordBuilder.Field(index).(*array.Uint32Builder).AppendValues(ui32array, nil)
		index++
	}
	if len(ui64array) != 0 {
		recordBuilder.Field(index).(*array.Uint64Builder).AppendValues(ui64array, nil)
		index++
	}
	if len(f32array) != 0 {
		recordBuilder.Field(index).(*array.Float32Builder).AppendValues(f32array, nil)
		index++
	}
	if len(f64array) != 0 {
		recordBuilder.Field(index).(*array.Float64Builder).AppendValues(f64array, nil)
		index++
	}
	if len(str) != 0 {
		recordBuilder.Field(index).(*array.StringBuilder).AppendValues(str, nil)
		index++
	}

	record := recordBuilder.NewRecord()
	defer record.Release()

	itr, err := array.NewRecordReader(arrowSchema, []array.Record{record})
	if err != nil {
		log.Fatal(err)
	}
	defer itr.Release()

	fmt.Println("\nCreating tables from the above records")

	table := array.NewTableFromRecords(arrowSchema, []array.Record{record})
	defer table.Release()

	tableReader := array.NewTableReader(table, 5)
	defer tableReader.Release()

	return tableReader
}

//ntuple struct to store data
type ntuple struct {
	n    int64
	cols []column
	args []rtree.ScanVar
	vars []interface{}
}

//add function to take up documentation
func (nt *ntuple) add(name string, leaf rtree.Leaf) {
	n := len(nt.cols)
	nt.cols = append(nt.cols, newColumn(name, leaf, nt.n))
	col := &nt.cols[n]
	nt.args = append(nt.args, rtree.ScanVar{Name: name, Leaf: leaf.Name()})
	nt.vars = append(nt.vars, col.data.Addr().Interface())
}

func (nt *ntuple) fill() {
	for i := range nt.cols {
		col := &nt.cols[i]
		col.fill()
	}
}

type column struct {
	name  string
	i     int64
	leaf  rtree.Leaf
	etype reflect.Type
	data  reflect.Value
	slice reflect.Value
}

func newColumn(name string, leaf rtree.Leaf, n int64) column {
	etype := leaf.Type()
	// if leaf.Len() > 1 && leaf.Kind() != reflect.String {
	// 	etype = reflect.ArrayOf(leaf.Len(), etype)
	// }

	rtype := reflect.SliceOf(etype)
	return column{
		name:  name,
		i:     0,
		leaf:  leaf,
		etype: etype,
		data:  reflect.New(etype).Elem(),
		slice: reflect.MakeSlice(rtype, int(n), int(n)),
	}
}

func (col *column) fill() {
	col.slice.Index(int(col.i)).Set(col.data)
	col.i++
}
