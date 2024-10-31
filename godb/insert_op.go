package godb

type InsertOp struct {
	insertFile DBFile
	operator   Operator
}

// Construct an insert operator that inserts the records in the child Operator
// into the specified DBFile.
func NewInsertOp(insertFile DBFile, child Operator) *InsertOp {
	return &InsertOp{insertFile: insertFile, operator: child}
}

// The insert TupleDesc is a one column descriptor with an integer field named "count"
func (i *InsertOp) Descriptor() *TupleDesc {
	return &TupleDesc{[]FieldType{{"count", "", IntType}}}
}

// Return an iterator function that inserts all of the tuples from the child
// iterator into the DBFile passed to the constuctor and then returns a
// one-field tuple with a "count" field indicating the number of tuples that
// were inserted.  Tuples should be inserted using the [DBFile.insertTuple]
// method.
func (iop *InsertOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	ran := false

	return func() (*Tuple, error) {
		if ran {
			return nil, nil
		}

		opIter, opIterError := iop.operator.Iterator(tid)

		if opIterError != nil {
			return nil, opIterError
		}

		count := 0

		for newTuple, newTupleError := opIter(); newTuple != nil || newTupleError != nil; newTuple, newTupleError = opIter() {
			insertError := iop.insertFile.insertTuple(newTuple, tid)

			if insertError != nil {
				return nil, insertError
			}

			count++
		}

		ran = true

		return &Tuple{*iop.Descriptor(), []DBValue{IntField{int64(count)}}, nil}, nil
	}, nil
}
