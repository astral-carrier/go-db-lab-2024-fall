package godb

import "sort"

//<silentstrip lab2>

//</silentstrip>

type OrderBy struct {
	orderBy []Expr // OrderBy should include these two fields (used by parser)
	child   Operator
	//add additional fields here
	ascending []bool
	len       int
	tuples    []*Tuple
}

// Construct an order by operator. Saves the list of field, child, and ascending
// values for use in the Iterator() method. Here, orderByFields is a list of
// expressions that can be extracted from the child operator's tuples, and the
// ascending bitmap indicates whether the ith field in the orderByFields list
// should be in ascending (true) or descending (false) order.
func NewOrderBy(orderByFields []Expr, child Operator, ascending []bool) (*OrderBy, error) {
	return &OrderBy{orderByFields, child, ascending, 0, nil}, nil
}

// Return the tuple descriptor.
//
// Note that the order by just changes the order of the child tuples, not the
// fields that are emitted.
func (o *OrderBy) Descriptor() *TupleDesc {
	return o.child.Descriptor()
}

// Return a function that iterates through the results of the child iterator in
// ascending/descending order, as specified in the constructor.  This sort is
// "blocking" -- it should first construct an in-memory sorted list of results
// to return, and then iterate through them one by one on each subsequent
// invocation of the iterator function.
//
// Although you are free to implement your own sorting logic, you may wish to
// leverage the go sort package and the [sort.Sort] method for this purpose. To
// use this you will need to implement three methods: Len, Swap, and Less that
// the sort algorithm will invoke to produce a sorted list. See the first
// example, example of SortMultiKeys, and documentation at:
// https://pkg.go.dev/sort
func (o *OrderBy) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	o.tuples = make([]*Tuple, 0)

	childIter, childIterError := o.child.Iterator(tid)

	if childIterError != nil {
		return nil, childIterError
	}

	o.len = 0

	for t, err := childIter(); t != nil || err != nil; t, err = childIter() {
		if err != nil {
			return nil, err
		}

		o.tuples = append(o.tuples, t)
		o.len++
	}

	sort.Sort(o)

	outputIndex := 0

	return func() (*Tuple, error) {
		if outputIndex >= o.len {
			return nil, nil
		}

		output := o.tuples[outputIndex]

		outputIndex++

		return output, nil
	}, nil
}

func (o *OrderBy) Len() int {
	return o.len
}

func (o *OrderBy) Less(i int, j int) bool {
	iElement := o.tuples[i]
	jElement := o.tuples[j]

	for index, criterion := range o.orderBy {
		iResult, _ := criterion.EvalExpr(iElement)
		jResult, _ := criterion.EvalExpr(jElement)

		if iResult.EvalPred(jResult, OpLt) {
			// less than means less if ascending and greater if not (matches value of ascending)
			return o.ascending[index]
		} else if iResult.EvalPred(jResult, OpGt) {
			// greater than means greater if ascending and less if not (negation of ascending)
			return !o.ascending[index]
		}

		// if neither of those above triggered, tie on this condition and go next
	}

	// true tie, choose false
	return false
}

func (o *OrderBy) Swap(i int, j int) {
	temp := o.tuples[i]
	o.tuples[i] = o.tuples[j]
	o.tuples[j] = temp
}
