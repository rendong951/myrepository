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
import java.util.*;

public class LineAnalyse {
    static class LineAnalyseMapper extends Mapper<LongWritable, Text, Text, SourceDataBean> {
        Text k = new Text();
        SourceDataBean v = new SourceDataBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String str = new String(value.getBytes(),0,value.getLength(),"GBK");
            String line = str.toString().replace("\"","");
            String[] fields = line.split(",");

            v.set(fields[0], fields[1], fields[2], fields[3], fields[4], fields[5]);
            k.set(v.getICId());
            context.write(k, v);

        }
    }

    static class LineAnalyseReducer extends Reducer<Text, SourceDataBean, NullWritable, PassengerLineBean> {
        PassengerLineBean v = new PassengerLineBean();

        @Override
        protected void reduce(Text key, Iterable<SourceDataBean> values, Context context)
                throws IOException, InterruptedException {
            ArrayList<SourceDataBean> beans = new ArrayList<SourceDataBean>();
            // 先将一个乘客的所有上车时间拿出来排序
            for (SourceDataBean bean : values) {
                SourceDataBean sourceDataBean = new SourceDataBean();
                try {
                    BeanCopier bc = BeanCopier.create(SourceDataBean.class, SourceDataBean.class, false);
                    bc.copy(bean, sourceDataBean, null);
                } catch(Exception e) {
                    e.printStackTrace();
                }
                beans.add(sourceDataBean);
            }

            //将bean按时间先后顺序排序
            Collections.sort(beans, new Comparator<SourceDataBean>() {

                @Override
                public int compare(SourceDataBean o1, SourceDataBean o2) {
                    try {
                        Date d1 = Utils.toDate(o1.getBoardingTime());
                        Date d2 = Utils.toDate(o2.getBoardingTime());
                        if (d1 == null || d2 == null) {
                            return 0;
                        }
                        return d1.compareTo(d2);
                    } catch (Exception e) {
                        e.printStackTrace();
                        return 0;
                    }
                }

            });

            /**
             * 以下逻辑为：从有序bean中分辨出各次行程，并对一次行程中所乘车按顺序标号step
             * 核心思想：
             * 就是比较相邻两条记录中的上车时间差，如果时间差<30分钟，则该两条记录属于同一个行程
             * 否则，就属于不同的行程
             *
             */
            try {
                int step = 1;
                String lineId = UUID.randomUUID().toString();
                for (int i = 0; i < beans.size(); i++) {
                    SourceDataBean bean = beans.get(i);
                    // 如果仅有1条数据，则直接输出
                    if (1 == beans.size()) {
                        // 设置默认停留时长为60s
                        v.set(lineId, key.toString(), bean.getBoardingTime(), bean.getLeavingTime(), bean.getBoardingStation(),
                                bean.getLeavingStation(), bean.getLine(), step);
                        context.write(NullWritable.get(), v);
                        lineId = UUID.randomUUID().toString();
                        break;
                    }

                    // 如果不止1条数据，则将第一条跳过不输出，遍历第二条时再输出
                    if (i == 0) {
                        continue;
                    }

                    // 求近上次下车和下次上车的时间差
                    long timeDiff = Utils.timeDiff(Utils.toDate(bean.getBoardingTime()), Utils.toDate(beans.get(i - 1).getLeavingTime()));

                    // 如果本次-上次时间差<30分钟，则输出前一次的乘车信息
                    if (timeDiff < 30 * 60 * 1000) {

                        v.set(lineId, key.toString(), beans.get(i - 1).getBoardingTime(), beans.get(i - 1).getLeavingTime(), beans.get(i - 1).getBoardingStation(),
                                beans.get(i - 1).getLeavingStation(), beans.get(i - 1).getLine(), step);
                        context.write(NullWritable.get(), v);
                        step++;
                    } else {

                        // 如果本次-上次时间差>30分钟，则输出前一次乘车信息且将step重置，以分隔为新的行程
                        v.set(lineId, key.toString(), beans.get(i - 1).getBoardingTime(), beans.get(i - 1).getLeavingTime(), beans.get(i - 1).getBoardingStation(),
                                beans.get(i - 1).getLeavingStation(), beans.get(i - 1).getLine(), step);
                        context.write(NullWritable.get(), v);
                        // 输出完上一条之后，重置step编号
                        step = 1;
                        lineId = UUID.randomUUID().toString();
                    }

                    // 如果此次遍历的是最后一条，则将本条直接输出
                    if (i == beans.size() - 1) {
                        // 设置默认停留时长为60s
                        v.set(lineId, key.toString(), bean.getBoardingTime(), bean.getLeavingTime(), bean.getBoardingStation(),
                                bean.getLeavingStation(), bean.getLine(), step);
                        context.write(NullWritable.get(), v);
                    }
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(LineAnalyse.class);

        job.setMapperClass(LineAnalyseMapper.class);
        job.setReducerClass(LineAnalyseReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SourceDataBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PassengerLineBean.class);

//		FileInputFormat.setInputPaths(job, new Path(args[0]));
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));

        FileInputFormat.setInputPaths(job, new Path("E:\\working\\dataanalyse\\201801.csv"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\working\\dataanalyse\\output"));

        job.waitForCompletion(true);

    }


}
