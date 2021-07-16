package com.pavan.mapreduce;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
  import org.apache.hadoop.hive.ql.metadata.HiveException;
  import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
  import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
  import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
  import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
  public class UDFArrayFirst extends GenericUDF {
  ListObjectInspector listInputObjectInspector;
  public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
  assert (args.length == 1); // This UDF accepts one argument
  // The first argument is a list

  assert(args[0].getCategory() == Category.LIST);
  listInputObjectInspector = (ListObjectInspector)args[0];
  /* Here comes the real usage for Object Inspectors : we know that our
  * return type is equal to the type of the elements of the input array.
  * We don't need to know in details what this type is, the
  * ListObjectInspector already has it */
  return listInputObjectInspector.getListElementObjectInspector();
}
public Object evaluate(DeferredObject[] args) throws HiveException {
if (args.length != 1) return null;

// Access the deferred value. Hive passes the arguments as "deferred" objects 
// to avoid some computations if we don't actually need some of the values
Object oin = args[0].get();

if (oin == null) return null;

int nbElements = listInputObjectInspector.getListLength(oin);
if (nbElements > 0) {
// The list is not empty, return its head
return  listInputObjectInspector.getListElement(oin, 0);
} else {
return null;
}
}

@Override
public String getDisplayString(String[] args) {
return "Here, write a nice description";
}
}