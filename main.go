package delay

import (
  "fmt"
  "math"
  "reflect"
  "errors"  
  "encoding/gob"  
  "appengine"
  "appengine/delay"
  "appengine/datastore"
  "appengine/search"
  "appengine/taskqueue"
)

var (
  putDelay *delay.Function
  deleteDelay *delay.Function  
  putMultiDelay *delay.Function
  deleteMultiDelay *delay.Function

  indexSearchDelay *delay.Function  
  indexSearchMultiDelay *delay.Function
  deIndexSearchDelay *delay.Function
  deIndexSearchMultiDelay *delay.Function
)

func GobRegister(obj interface{}) {
  gob.Register(obj)
}

func init() {
  putDelay = delay.Func("putDelay", func(c appengine.Context, key *datastore.Key, input interface{}) error {
    _, err := datastore.Put(c, key, input)
    if err != nil {
      return err
    }
    return nil
  })    

  putMultiDelay = delay.Func("putMultiDelay", func(c appengine.Context, keys []*datastore.Key, input interface{}) error {
    // c.Infof("run putMultiDelay")
    _, err := datastore.PutMulti(c, keys, input)
    if err != nil {
      return err
    }
    return nil
  })    

  deleteDelay = delay.Func("deleteDelay", func(c appengine.Context, key *datastore.Key) error {
    err := datastore.Delete(c, key)
    if err != nil {
      return err
    }
    return nil
  })   

  deleteMultiDelay = delay.Func("deleteMultiDelay", func(c appengine.Context, keys []*datastore.Key) error {
    err := datastore.DeleteMulti(c, keys)
    if err != nil {
      return err
    }
    return nil
  })
  
  indexSearchDelay = delay.Func("indexSearchDelay", func(c appengine.Context, doc string, id string, data interface{}) error {
    index, err := search.Open(doc)
    if err != nil {
      return err
    }
  
    _, err = index.Put(c, id, data)
    if err != nil {
      return err
    }
    return nil
  })
  
  indexSearchMultiDelay = delay.Func("indexSearchMultiDelay", func(c appengine.Context, doc string, ids []string, data interface{}) error {
    index, err := search.Open(doc)
    if err != nil {
      return err
    }
    
    for i:=0;i<len(ids);i++ {
      id := ids[i]
      searchDoc := reflect.ValueOf(data).Index(i).Interface()
      _, err = index.Put(c, id, searchDoc)
      if err != nil {
        return err
      }
    }
    return nil
  })  
  
  deIndexSearchDelay = delay.Func("deIndexSearchDelay", func(c appengine.Context, doc string, id string) error {
    index, err := search.Open(doc)
    if err != nil {
      return err
    }
  
    err = index.Delete(c, id)
    if err != nil {
      return err
    }
    return nil
  })
  
  deIndexSearchMultiDelay = delay.Func("deIndexSearchMultiDelay", func(c appengine.Context, doc string, ids []string) error {
    index, err := search.Open(doc)
    if err != nil {
      return err
    }
    for _, id := range ids {
      err = index.Delete(c, id)
      if err != nil {
        return err
      } 
    }
    return nil
  })
}

func PutDelay(c appengine.Context, key *datastore.Key, data interface{}) error {
  c.Debugf("PutDelay: type=%s", reflect.ValueOf(data).Type().Name())  
  task, err  := putDelay.Task(key, data)
  if err != nil {
    return err
  }
  _, err = taskqueue.Add(c, task, TASKQUEUE_NAME)
  if err != nil {
    return err
  }
  return nil
}

func PutMultiDelay(c appengine.Context, keys []*datastore.Key, data interface{}, batchSize int) error {
  if len(keys) == 0 {
    return nil
  }
  dataValue := reflect.ValueOf(data)
  
  c.Debugf("PutMultiDelay: type=%+v size=%d", dataValue.Type(), len(keys))  
  
  if len(keys) != dataValue.Len() {
    return errors.New("keys and entities slice must have same length")
  }
  batchCount := int(math.Ceil(float64(len(keys))/float64(batchSize)))
  tasks := make([]*taskqueue.Task, 0, batchCount)

  for i:=0;i<dataValue.Len();i+=batchSize {
    start := i
    end := start + batchSize
    if end > len(keys) {
      end = len(keys)
    }
    sliceValue := reflect.MakeSlice(reflect.SliceOf( dataValue.Index(0).Type()), 0, end - start)
    sliceValue = reflect.AppendSlice(sliceValue, dataValue.Slice(start, end))    
    // putMultiDelay.Call(c, keys[start:end], sliceValue.Interface())
    task, err := putMultiDelay.Task(keys[start:end], sliceValue.Interface())
    if err != nil {
      return err
    }
    tasks = append(tasks, task)
  }
  _, err := taskqueue.AddMulti(c, tasks, TASKQUEUE_NAME)
  if err != nil {
    return err
  }
  
  return nil
}

func DeleteDelay(c appengine.Context, key *datastore.Key) error {
  task, err := deleteDelay.Task(key)
  if err != nil {
    return err
  }
  _, err = taskqueue.Add(c, task, TASKQUEUE_NAME)
  if err != nil {
    return err
  }
  return nil
}

func DeleteMultiDelay(c appengine.Context, keys []*datastore.Key, batchSize int) error {
  if len(keys) == 0 {
    return nil
  }
  batchCount := int(math.Ceil(float64(len(keys))/float64(batchSize)))
  tasks := make([]*taskqueue.Task, 0, batchCount)

  for i:=0;i<len(keys);i+=batchSize {
    start := i
    end := start + batchSize
    if end > len(keys) {
      end = len(keys)
    }
    // deleteMultiDelay.Call(c, keyBatch)    
    task, err := deleteMultiDelay.Task(keys[start:end])
    if err != nil {
      return err
    }
    tasks = append(tasks, task)
  }
  _, err := taskqueue.AddMulti(c, tasks, TASKQUEUE_NAME)
  if err != nil {
    return err
  }
  return nil
}

func IndexSearchMultiDelay(c appengine.Context, doc string, ids []string, data interface{}, batchSize int) error {
  if len(ids) == 0 {
    return nil
  }
  dataValue := reflect.ValueOf(data)
  c.Debugf("IndexSearchMultiDelay: type=%+v size=%d", dataValue.Type(), len(ids))    
  
  if len(ids) != dataValue.Len() {
    return errors.New("ids and search doc slice must have same length")
  }
  batchCount := int(math.Ceil(float64(len(ids))/float64(batchSize)))
  tasks := make([]*taskqueue.Task, 0, batchCount)

  for i:=0;i<dataValue.Len();i+=batchSize {
    start := i
    end := start + batchSize
    if end > len(ids) {
      end = len(ids)
    }
    sliceValue := reflect.MakeSlice(reflect.SliceOf( dataValue.Index(0).Type()), 0, end - start)
    sliceValue = reflect.AppendSlice(sliceValue, dataValue.Slice(start, end))    
    // putMultiDelay.Call(c, keys[start:end], sliceValue.Interface())
    task, err := indexSearchMultiDelay.Task(doc, ids[start:end], sliceValue.Interface())
    if err != nil {
      return err
    }
    tasks = append(tasks, task)
  }
  _, err := taskqueue.AddMulti(c, tasks, TASKQUEUE_NAME)
  if err != nil {
    return err
  }
  return nil
}

func IndexSearchDelay(c appengine.Context, doc string, id string, data interface{}) error {
  task, err := indexSearchDelay.Task(doc, id, data)
  if err != nil {
    return err
  }
  _, err = taskqueue.Add(c, task, TASKQUEUE_NAME)
  if err != nil {
    return err
  }
  return nil
}

func DeIndexSearchMultiDelay(c appengine.Context, doc string, ids []string, batchSize int) error {
  if len(ids) == 0 {
    return nil
  }
  c.Debugf("DeIndexSearchMultiDelay: size=%d", len(ids))      
  
  batchCount := int(math.Ceil(float64(len(ids))/float64(batchSize)))
  tasks := make([]*taskqueue.Task, 0, batchCount)

  for i:=0;i<len(ids);i+=batchSize {
    start := i
    end := start + batchSize
    if end > len(ids) {
      end = len(ids)
    }
    // deleteMultiDelay.Call(c, keyBatch)    
    task, err := deIndexSearchMultiDelay.Task(doc, ids[start:end])
    if err != nil {
      return err
    }
    tasks = append(tasks, task)
  }
  _, err := taskqueue.AddMulti(c, tasks, TASKQUEUE_NAME)
  if err != nil {
    return err
  }
  return nil
}

func DeIndexSearchDelay(c appengine.Context, doc string, id string) error {
  task, err := deIndexSearchDelay.Task(doc, id)
  if err != nil {
    return err
  }
  _, err = taskqueue.Add(c, task, TASKQUEUE_NAME)
  if err != nil {
    return err
  }
  return nil
}