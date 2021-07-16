package com.pavan.mapreduce;
import java.util.ArrayList;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;


public class ExpandNameDetail extends GenericUDTF{
        private Object[] fwdObj = null;
        private PrimitiveObjectInspector nameDtlOI = null;
        public StructObjectInspector initialize(ObjectInspector[] arg)
        {
                ArrayList<String> fieldNames = new ArrayList<String>();
                ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
                nameDtlOI = (PrimitiveObjectInspector) arg[0];
                fieldNames.add("FirstName");
                fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                PrimitiveCategory.STRING));
                fieldNames.add("LastName");
                fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                PrimitiveCategory.STRING));
                fwdObj = new Object[2];
                return ObjectInspectorFactory.getStandardStructObjectInspector(
                                fieldNames, fieldOIs);
        }
        public void process(Object[] record) throws HiveException
        {
                String nameDtl = nameDtlOI.getPrimitiveJavaObject(record[0]).toString();

                String str[] = nameDtl.split(" ");
                fwdObj[0] = str[0];
                fwdObj[1] = str[1];

                this.forward(fwdObj);

        }

        public void close()
        {
                  
        }
}