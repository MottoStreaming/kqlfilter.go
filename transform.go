package kqlfilter

type NodeMapper struct {
	TransformIdentifierFunc func(string) string
	TransformValueFunc      func(string) string
}

func NewNodeMapper() NodeMapper {
	return NodeMapper{
		TransformIdentifierFunc: func(s string) string {
			return s
		},
		TransformValueFunc: func(s string) string {
			return s
		},
	}
}

func (m NodeMapper) Map(ast Node) error {
	switch x := ast.(type) {
	case *AndNode:
		for _, n := range x.Nodes {
			err := m.Map(n)
			if err != nil {
				return err
			}
		}
	case *OrNode:
		for _, n := range x.Nodes {
			err := m.Map(n)
			if err != nil {
				return err
			}
		}
	case *IsNode:
		x.Identifier = m.TransformIdentifierFunc(x.Identifier)

		err := m.Map(x.Value)
		if err != nil {
			return err
		}
	case *NotNode:
		err := m.Map(x.Expr)
		if err != nil {
			return err
		}
	case *RangeNode:
		x.Identifier = m.TransformIdentifierFunc(x.Identifier)

		err := m.Map(x.Value)
		if err != nil {
			return err
		}
	case *LiteralNode:
		x.Value = m.TransformValueFunc(x.Value)
	}

	return nil
}
