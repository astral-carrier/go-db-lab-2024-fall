package godb

type DeleteOp struct {
	deleteFile DBFile
	operator   Operator
}

// Construct a delete operator. The delete operator deletes the records in the
// child Operator from the specified DBFile.
func NewDeleteOp(deleteFile DBFile, child Operator) *DeleteOp {
	return &DeleteOp{deleteFile: deleteFile, operator: child}
}

// The delete TupleDesc is a one column descriptor with an integer field named
// "count".
func (i *DeleteOp) Descriptor() *TupleDesc {
	return &TupleDesc{[]FieldType{{"count", "", IntType}}}
}

// Return an iterator that deletes all of the tuples from the child iterator
// from the DBFile passed to the constructor and then returns a one-field tuple
// with a "count" field indicating the number of tuples that were deleted.
// Tuples should be deleted using the [DBFile.deleteTuple] method.
func (dop *DeleteOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	ran := false

	return func() (*Tuple, error) {
		if ran {
			return nil, nil
		}

		opIter, opIterError := dop.operator.Iterator(tid)

		if opIterError != nil {
			return nil, opIterError
		}

		count := 0

		for newTuple, newTupleError := opIter(); newTuple != nil || newTupleError != nil; newTuple, newTupleError = opIter() {
			deleteError := dop.deleteFile.deleteTuple(newTuple, tid)

			if deleteError != nil {
				return nil, deleteError
			}

			count++
		}

		ran = true

		return &Tuple{*dop.Descriptor(), []DBValue{IntField{int64(count)}}, nil}, nil
	}, nil
}
