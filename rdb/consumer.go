package rdb

import (
	"log"
)

type AuxiliaryField struct {
	K string
	V interface{}
}

type Consumer interface {
	RDBVersion(version string)
	AuxiliaryField(field AuxiliaryField)
	ResizeDB(dbHashtableSize uint32, expiryHashtableSize uint32)
	SelectDB(db uint32)
	Row(row *Row)
	End(crc []byte)
}

type LogConsumer struct {
	n uint64
}

func (c *LogConsumer) RDBVersion(version string) {
	log.Printf("Version: %s\n", version)
}

func (c *LogConsumer) AuxiliaryField(field AuxiliaryField) {
	log.Printf("Aux: %+v\n", field)
}

func (c *LogConsumer) ResizeDB(dbHashtableSize uint32, expiryHashtableSize uint32) {
	log.Printf("ResizeDB: %d %d\n", dbHashtableSize, expiryHashtableSize)
}

func (c *LogConsumer) SelectDB(db uint32) {
	log.Printf("SelectDB: %d\n", db)
}

func (c *LogConsumer) Row(row *Row) {
	c.n++
	// log.Printf("Row: %#v\n", row)
	if c.n%100000 == 0 {
		log.Println(c.n/1000000)
	}
}

func (c *LogConsumer) End(crc []byte) {
	log.Println(c.n)
	log.Printf("End, crc: %x", crc)
}
