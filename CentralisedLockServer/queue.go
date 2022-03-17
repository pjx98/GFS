package CentralisedLockServer

import(
	"errors"
)

type Queue struct {
	Elements []int
}

// Enqueue - adds an element to the front of the queue
func (queue Queue) Enqueue(elem int) {
	queue.Elements = append(queue.Elements, elem)
}

// Dequeue - removes the first element from a queue
func (queue Queue) Dequeue() (int, error) {
	if len(queue.Elements) == 0 {
		return 0, errors.New("empty queue")
	}
	var popped_element int
	popped_element, queue.Elements = queue.Elements[0], queue.Elements[1:]
	return popped_element, nil
}

// Peek - returns the first element from our queue without updating queue
func (queue *Queue) Peek() (int, error) {
	if queue.IsEmpty() {
		return 0, errors.New("empty queue")
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