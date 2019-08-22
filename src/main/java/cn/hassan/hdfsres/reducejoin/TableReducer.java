package cn.hassan.hdfsres.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {

        List<TableBean> orderBeans = new ArrayList<TableBean>();
        final TableBean pdBean = new TableBean();

        for (TableBean value : values) {
            if (value.getFlag() == 0) {
                TableBean bean = new TableBean();
                try {
                    BeanUtils.copyProperties(bean,value);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                orderBeans.add(bean);
            }else {
                try {
                    BeanUtils.copyProperties(pdBean,value);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        orderBeans.forEach(e -> {
            e.setpName(pdBean.getpName());
            try {
                context.write(e,NullWritable.get());
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        });
    }
}
