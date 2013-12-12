package counter

import "sort"

type Counter struct {
	m map[string]int
}
type CounterItem struct {
	Key string
	Value int
}
func NewCounter() *Counter {
	counter := new(Counter)
	counter.m = make(map[string]int)
	return counter
}
func (counter Counter) Count(k string) {
	v, ok := counter.m[k]
	if ! ok {
		v = 0
	}
	v++
	counter.m[k] = v
}

type CounterItemList []CounterItem
func (cil CounterItemList) Len() int {
	return len(cil)
}
func (cil CounterItemList) Less(i int, j int) bool {
	return cil[i].Value >= cil[j].Value
}
func (cil CounterItemList) Swap(i int, j int) {
	cil[i], cil[j] = cil[j], cil[i]
}

func (counter Counter) MostCommon() CounterItemList {
    res := make(CounterItemList, len(counter.m))
    i := 0
    for k, v := range counter.m {
        res[i] = CounterItem{k, v}
        i++
    }
    sort.Sort(res)
    return res
}

func (counter Counter) Sum() int {
	res := 0
	for _, v := range counter.m {
		res += v
	}
	return res
}

func (counter Counter) Get(key string) int {
	val, exists := counter.m[key]
	if ! exists {
		panic("Expected key " + key + " to be there")
	}
	return val
}

func (counter Counter) Keys() []string {
	res := make([]string, 0)
	for k, _ := range counter.m {
		res = append(res, k)
	}
	return res
}
