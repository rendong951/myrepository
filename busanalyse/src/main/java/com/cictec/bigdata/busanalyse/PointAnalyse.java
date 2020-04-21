package com.cictec.bigdata.busanalyse;

import net.sf.cglib.beans.BeanCopier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class PointAnalyse {
    static class PointAnalyseMapper extends Mapper<LongWritable, Text, Text, PassengerLineBean> {
        PassengerLineBean plb = new PassengerLineBean();
        Text k = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] field = value.toString().split("\001");
            plb.set(field[0], field[1], field[2], field[3], field[4], field[5], field[6], Integer.parseInt(field[7]));
            k.set(plb.getLineId());
            context.write(k, plb);
        }

    }

    static class PointAnalyseReducer extends Reducer<Text, PassengerLineBean, NullWritable, PointBean> {

        PointBean pointBean = new PointBean();
        @Override
        protected void reduce(Text key, Iterable<PassengerLineBean> plBeans, Context context)
                throws IOException, InterruptedException{
            //将pvBeans按照step排序
            ArrayList<PassengerLineBean> plBeansList = new ArrayList<PassengerLineBean>();
            for (PassengerLineBean plBean : plBeans) {
                PassengerLineBean bean = new PassengerLineBean();
                try {
                    BeanCopier bc = BeanCopier.create(PassengerLineBean.class, PassengerLineBean.class, false);
                    bc.copy(plBean, bean, null);
//                    BeanUtils.copyProperties(bean, plBean);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                plBeansList.add(bean);
            }

            Collections.sort(plBeansList, new Comparator<PassengerLineBean>(){
                @Override
                public int compare(PassengerLineBean o1, PassengerLineBean o2) {
                    return o1.getStep() > o2.getStep() ? 1 : -1;
                }
            });

            try {
                // 取行程的首记录
                pointBean.setBoardingStation(plBeansList.get(0).getBoardingStation());
                pointBean.setBoardingTime(plBeansList.get(0).getBoardingTime());
                // 取行程的尾记录
                pointBean.setLeavingStation(plBeansList.get(plBeansList.size() - 1).getLeavingStation());
                pointBean.setLeavingTime(plBeansList.get(plBeansList.size() - 1).getLeavingTime());

                // 行程的换乘次数
                pointBean.setTransfer(plBeansList.size() - 1);
                // 来访者的ip
                pointBean.setICId(plBeansList.get(0).getICId());
                //乘车总时长
                long timeDiff = Utils.timeDiff(Utils.toDate(plBeansList.get(plBeansList.size() - 1).getLeavingTime()),
                        Utils.toDate(plBeansList.get(0).getBoardingTime()));
                pointBean.setTotalTime(String.valueOf(timeDiff/1000));

                pointBean.setLineId(key.toString());

                context.write(NullWritable.get(), pointBean);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(PointAnalyse.class);

        job.setMapperClass(PointAnalyseMapper.class);
        job.setReducerClass(PointAnalyseReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PassengerLineBean.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(PointBean.class);


//		FileInputFormat.setInputPaths(job, new Path(args[0]));
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileInputFormat.setInputPaths(job, new Path("E:\\working\\dataanalyse\\output\\part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\working\\dataanalyse\\output2"));

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);

    }
}
