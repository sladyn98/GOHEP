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

	var fields = make([]arrow.Field, len(tree.Branches()))

	for i, branches := range tree.Branches() {
		fmt.Println(branches.Name())

		switch branches.GoType().Kind() {

		case reflect.Int8:
			fields[i].Name = branches.Name()
			fields[i].Type = arrow.PrimitiveTypes.Int8

		case reflect.Int16:
			fields[i].Name = branches.Name()
			fields[i].Type = arrow.PrimitiveTypes.Int16

		case reflect.Int32:
			fields[i].Name = branches.Name()
			fields[i].Type = arrow.PrimitiveTypes.Int32

		case reflect.Int64:
			fields[i].Name = branches.Name()
			fields[i].Type = arrow.PrimitiveTypes.Int64

		case reflect.Float32:
			fields[i].Name = branches.Name()
			fields[i].Type = arrow.PrimitiveTypes.Float32

		case reflect.Float64:
			fields[i].Name = branches.Name()
			fields[i].Type = arrow.PrimitiveTypes.Float64

		case reflect.String:
			fields[i].Name = branches.Name()
			fields[i].Type = arrow.BinaryTypes.String

		default:
			panic("unmatched data type")
		}

	}

	arrowSchema := arrow.NewSchema(fields, nil)
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

	// Create memory pool and record builder
	pool := memory.NewGoAllocator()
	recordBuilder := array.NewRecordBuilder(pool, arrowSchema)
	defer recordBuilder.Release()

	//Scan the n tuples
	sc, err := rtree.NewTreeScannerVars(tree, nt.args...)
	if err != nil {
		log.Fatal(err)
	}
	defer sc.Close()
	nrows := 0
	index := 0
	for sc.Next() {
		err = sc.Scan(nt.vars...)
		if err != nil {
			log.Fatal(err)
		}

		for i := range nt.cols {
			col := &nt.cols[i]
			// fmt.Println("look at the data", col.name)

			switch colDataType := col.name; colDataType {

			case "Int8":
				recordBuilder.Field(index % len(tree.Branches())).(*array.Int8Builder).Append(col.data.Interface().(int8))
				index++

			case "Int16":
				recordBuilder.Field(index % len(tree.Branches())).(*array.Int16Builder).Append(col.data.Interface().(int16))
				index++

			case "Int32":
				recordBuilder.Field(index % len(tree.Branches())).(*array.Int32Builder).Append(col.data.Interface().(int32))
				index++

			case "Int64":
				recordBuilder.Field(index % len(tree.Branches())).(*array.Int64Builder).Append(col.data.Interface().(int64))
				index++

			case "UInt8":
				recordBuilder.Field(index % len(tree.Branches())).(*array.Int8Builder).Append(int8(col.data.Interface().(int8)))
				index++

			case "UInt16":
				recordBuilder.Field(index % len(tree.Branches())).(*array.Int16Builder).Append(int16(col.data.Interface().(int16)))
				index++

			case "UInt32":
				recordBuilder.Field(index % len(tree.Branches())).(*array.Int32Builder).Append(int32(col.data.Interface().(int32)))
				index++

			case "UInt64":
				recordBuilder.Field(index % len(tree.Branches())).(*array.Int64Builder).Append(int64(col.data.Interface().(int64)))
				index++

			case "Float32":
				recordBuilder.Field(index % len(tree.Branches())).(*array.Float32Builder).Append(col.data.Interface().(float32))
				index++

			case "Float64":
				recordBuilder.Field(index % len(tree.Branches())).(*array.Float64Builder).Append(col.data.Interface().(float64))
				index++

			case "Str":
				recordBuilder.Field(index % len(tree.Branches())).(*array.StringBuilder).Append(col.data.Interface().(string))
				index++

			}

		}
		nrows++
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
