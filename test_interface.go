package main

import (
	"fmt"
)

type i interface {
	Encode() []byte
}

type xs struct {
	c1 string
	c2 i
}
type StringEncoder string
type ByteEncoder   []byte
type PasaAint	   int

func (s StringEncoder) Encode() []byte {
	return []byte(s)
}	

func main() {
	m := xs{}
	m.c1 = "hola"
	m.c2 = StringEncoder("c2")
	a := StringEncoder("a")
	b := ByteEncoder("b")
	c := PasaAint(3)

	fmt.Println(a,b,c,m)
}

