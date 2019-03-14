package main

import (
	"GOHEP/rarrow"
	"fmt"
	"log"

	"go-hep.org/x/hep/groot"
	"go-hep.org/x/hep/groot/rtree"
)

func main() {

	file, err := groot.Open("data.root")
	if err != nil {
		log.Fatalf("could not open file : %v", err)
	}
	defer file.Close()

	fmt.Printf("rkeys: %d\n", len(file.Keys()))

	for _, k := range file.Keys() {
		fmt.Printf("key: name=%q, type=%q, title=%q , type=%q\n", k.Name(), k.ClassName(), k.Title(), k.ObjectType())
	}

	obj, err := file.Get("tree")

	if err != nil {
		fmt.Println(err)
	}

	tree, ok := obj.(rtree.Tree)
	if !ok {
		log.Fatalf("Error in creating rtree.Tree object")
	}

	fmt.Println("Creating arrow schema...................")

	arrowSchema := rarrow.CreateArrowSchema(tree)

	fmt.Println(arrowSchema)

	fmt.Println("Arrow schema created.......")

	fmt.Println("Creating Table reader......")
	tableReader := rarrow.CreateTableReader(arrowSchema, tree)

	fmt.Println("Reading tables.........")
}
