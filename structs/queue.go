package structs

import(
	"errors"
)

type Queue struct {
	Elements []string
}

// Enqueue - adds an element to the front of the queue
func (queue Queue) Enqueue(elem string) {
	queue.Elements = append(queue.Elements, elem)
}

// Dequeue - removes the first element from a queue
func (queue Queue) Dequeue() (string, error) {
	if len(queue.Elements) == 0 {
		return "None", errors.New("empty queue")
	}
	var popped_element string
	popped_element, queue.Elements = queue.Elements[0], queue.Elements[1:]
	return popped_element, nil
}

// Peek - returns the first element from our queue without updating queue
func (queue Queue) Peek() (string, error) {
	if queue.IsEmpty() {
		return "None", errors.New("empty queue")
	}
	return queue.Elements[0], nil
}

// GetLength - returns the length of the queue
func (queue Queue) GetLength() int {
	return len(queue.Elements)
}

// LastElem - returns the last element of the queue
func (queue Queue) IsEmpty() bool {
	return len(queue.Elements) == 0
} 