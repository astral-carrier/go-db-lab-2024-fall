package godb

type Project struct {
	selectFields []Expr // required fields for parser
	outputNames  []string
	child        Operator
	distinct     bool
	//add additional fields here

}

// Construct a projection operator. It saves the list of selected field, child,
// and the child op. Here, selectFields is a list of expressions that represents
// the fields to be selected, outputNames are names by which the selected fields
// are named (should be same length as selectFields; throws error if not),
// distinct is for noting whether the projection reports only distinct results,
// and child is the child operator.
func NewProjectOp(selectFields []Expr, outputNames []string, distinct bool, child Operator) (Operator, error) {
	return &Project{selectFields, outputNames, child, distinct}, nil
}

// Return a TupleDescriptor for this projection. The returned descriptor should
// contain fields for each field in the constructor selectFields list with
// outputNames as specified in the constructor.
//
// HINT: you can use expr.GetExprType() to get the field type
func (p *Project) Descriptor() *TupleDesc {
	fieldDescs := make([]FieldType, 0)

	for index, field := range p.selectFields {
		newFieldType := field.GetExprType()

		newFieldType.Fname = p.outputNames[index]

		fieldDescs = append(fieldDescs, newFieldType)
	}

	return &TupleDesc{fieldDescs}
}

// Project operator implementation. This function should iterate over the
// results of the child iterator, projecting out the fields from each tuple. In
// the case of distinct projection, duplicate tuples should be removed. To
// implement this you will need to record in some data structure with the
// distinct tuples seen so far. Note that support for the distinct keyword is
// optional as specified in the lab 2 assignment.
func (p *Project) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	childIter, childIterError := p.child.Iterator(tid)

	if childIterError != nil {
		return nil, childIterError
	}
	if childIter == nil {
		return nil, GoDBError{MalformedDataError, "child iter unexpectedly nil"}
	}

	projectFields := make([]FieldType, 0)

	for _, field := range p.selectFields {
		projectFields = append(projectFields, field.GetExprType())
	}

	return func() (*Tuple, error) {
		t, err := childIter()

		if err != nil {
			return nil, err
		}
		if t == nil {
			return nil, nil
		}

		projection, projectError := t.project(projectFields)

		if projectError != nil {
			return nil, projectError
		}

		projection.Desc = *p.Descriptor()

		return projection, nil
	}, nil
}
