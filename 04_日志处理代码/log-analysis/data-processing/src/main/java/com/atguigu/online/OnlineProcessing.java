package com.atguigu.online;

import com.atguigu.model.StartupReportLogs;
import com.atguigu.model.UserCityStatModel;
import com.atguigu.service.BehaviorStatService;
import com.atguigu.utils.JSONUtil;
import com.atguigu.utils.PropertiesUtil;
import com.atguigu.utils.StringUtil;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConversions;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class OnlineProcessing {

    public static void main(String[] args) throws Exception {
        // 获取配置文件的全部配置
        Properties properties = PropertiesUtil.getProperties("" +
                "data-processing/src/main/resources/config.properties");

        // 获取checkpoint目录
        String checkpointPath = properties.getProperty("streaming.checkpoint.path");

        // Scala: getActiveOrCreate
        JavaStreamingContext javaStreamingContext = JavaStreamingContext.getOrCreate(checkpointPath, createStreamingContext(properties));

        javaStreamingContext.start();

        javaStreamingContext.awaitTermination();
    }

    private static Function0<JavaStreamingContext> createStreamingContext(final Properties properties) {

        Function0<JavaStreamingContext> func = new Function0<JavaStreamingContext>() {
            @Override
            public JavaStreamingContext call() throws Exception {

                // 第一步：创建sparkConf
                SparkConf sparkConf = new SparkConf().setAppName("online").setMaster("local[*]");
                // 设置streaming优雅的停止
                sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true");
                // 设置streaming程序每秒钟最多从partition消费多少条数据
                sparkConf.set("spark.streaming.maxRatePerPartition", "100");
                // 设置一下序列化方式——Kryo
                sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
                // 指定Kryo的注册器的全类名
                sparkConf.set("spark.kryo.registrator", "com.atguigu.registrator.MyKryoRegistrator");

                // 第二步：创建streamingContext
                String intervel = properties.getProperty("streaming.interval");
                final Long streamingInterval = Long.parseLong(intervel);
                JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(streamingInterval));

                // 第三步：获取kafka相关的配置信息
                final String kafkaBrokers = properties.getProperty("kafka.broker.list");
                final String groupId = properties.getProperty("kafka.groupId");
                String kafkaTopic = properties.getProperty("kafka.topic");
                final Set<String> topics = new HashSet<>(Arrays.asList(kafkaTopic.split(",")));
                Map<String, String> kafkaParam = new HashMap<>();
                kafkaParam.put("metadata.broker.list", kafkaBrokers);
                kafkaParam.put("group.id", groupId);

                // 第四步：获取之前保存在zk的对应消费者组以及对应主题+分区的offset值
                final KafkaCluster kafkaCluster = getKafkaCluster(kafkaParam);
                Map<TopicAndPartition, Long> offsets = getConsumerOffsets(kafkaCluster, groupId, topics);

                // 第五步：创建DirectDStream
                JavaInputDStream<String> kafkaDStream = KafkaUtils.createDirectStream(
                        javaStreamingContext,
                        String.class,
                        String.class,
                        StringDecoder.class,
                        StringDecoder.class,
                        String.class,
                        kafkaParam,
                        offsets,
                        new Function<MessageAndMetadata<String, String>, String>() {
                            @Override
                            public String call(MessageAndMetadata<String, String> v1) throws Exception {
                                return v1.message();
                            }
                        }
                );

                // 第六步：获取RDD中维护的offset数据
                final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();
                // kafkaDStream.tranform{
                // dstreamRdd => (HasOffsetRanges)dstreamRdd.offsetRanges()
                // ......
                // }
                JavaDStream<String> kafkaOriDStream = kafkaDStream.transform(
                        new Function<JavaRDD<String>, JavaRDD<String>>() {
                            @Override
                            public JavaRDD<String> call(JavaRDD<String> dstreamRdd) throws Exception {
                                OffsetRange[] offsets = ((HasOffsetRanges)dstreamRdd.rdd()).offsetRanges();
                                offsetRanges.set(offsets);
                                return dstreamRdd;
                            }
                        }
                );

                // 第七步：对数据进行过滤
                JavaDStream<String> kafkaFilteredDStream = kafkaDStream.filter(
                        new Function<String, Boolean>() {
                            @Override
                            public Boolean call(String message) throws Exception {

                                // 过滤掉不属于三种核心日志的所有数据
                                if(!message.contains("appVersion") && !message.contains("currentPage") &&
                                        !message.contains("errorMajor")){
                                    return false;
                                }

                                // 本项目只使用启动日志，过滤掉其他两种日志
                                if(!message.contains("appVersion")){
                                    return false;
                                }

                                // 将json字符串转化为对象
                                StartupReportLogs startupReportLogs = JSONUtil.json2Object(message, StartupReportLogs.class);

                                // 将无法实例化或者关键字段非法的数据过滤掉
                                if(startupReportLogs == null || StringUtil.isEmpty(startupReportLogs.getUserId()) ||
                                        StringUtil.isEmpty(startupReportLogs.getAppId())){
                                    return false;
                                }

                                return true;
                            }
                        }
                );

                // 第八步：实时统计每个城市当前的点击次数
                //  (city ,count)
                JavaPairDStream<String, Long> kafkaCountDStream = kafkaFilteredDStream.mapToPair(
                        new PairFunction<String, String, Long>() {
                            @Override
                            public Tuple2<String, Long> call(String message) throws Exception {

                                StartupReportLogs startupReportLogs = JSONUtil.json2Object(message, StartupReportLogs.class);

                                String city = startupReportLogs.getCity();

                                Tuple2<String, Long> tuple2 = new Tuple2<String, Long>(city, 1L);

                                return tuple2;
                            }
                        }
                ).reduceByKey(new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                });

                kafkaCountDStream.foreachRDD(
                        new VoidFunction<JavaPairRDD<String, Long>>() {
                            @Override
                            // RDD[(city, count)]
                            public void call(JavaPairRDD<String, Long> rdd) throws Exception {
                                rdd.foreachPartition(
                                        new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                                            @Override
                                            public void call(Iterator<Tuple2<String, Long>> items) throws Exception {
                                                BehaviorStatService behaviorStatService = BehaviorStatService.getInstance(properties);
                                                while(items.hasNext()){
                                                    UserCityStatModel model = new UserCityStatModel();

                                                    Tuple2<String, Long> cityCount = items.next();

                                                    String city = cityCount._1;
                                                    Long count = cityCount._2;

                                                    model.setCity(city);
                                                    model.setNum(count);

                                                    behaviorStatService.addUserNumOfCity(model);
                                                }
                                            }
                                        }
                                );
                                offsetToZk(kafkaCluster, offsetRanges, groupId);
                            }
                        }
                );
                return javaStreamingContext;
            }
        };
        return func;
    }

    /*
* 将offset写入zk
* */
    public static void offsetToZk(final KafkaCluster kafkaCluster,
                                  final AtomicReference<OffsetRange[]> offsetRanges,
                                  final String groupId) {
        // 遍历每一个偏移量信息
        for (OffsetRange o : offsetRanges.get()) {

            // 提取offsetRange中的topic和partition信息封装成TopicAndPartition
            TopicAndPartition topicAndPartition = new TopicAndPartition(o.topic(), o.partition());
            // 创建Map结构保持TopicAndPartition和对应的offset数据
            Map<TopicAndPartition, Object> topicAndPartitionObjectMap = new HashMap();
            // 将当前offsetRange的topicAndPartition信息和untilOffset信息写入Map
            topicAndPartitionObjectMap.put(topicAndPartition, o.untilOffset());

            // 将Java的Map结构转换为Scala的mutable.Map结构
            scala.collection.mutable.Map<TopicAndPartition, Object> testMap = JavaConversions.mapAsScalaMap(topicAndPartitionObjectMap);

            // 将Scala的mutable.Map转化为imutable.Map
            scala.collection.immutable.Map<TopicAndPartition, Object> scalatopicAndPartitionObjectMap =
                    testMap.toMap(new Predef.$less$colon$less<Tuple2<TopicAndPartition, Object>, Tuple2<TopicAndPartition, Object>>() {
                        public Tuple2<TopicAndPartition, Object> apply(Tuple2<TopicAndPartition, Object> v1) {
                            return v1;
                        }
                    });

            // 更新offset到kafkaCluster
            kafkaCluster.setConsumerOffsets(groupId, scalatopicAndPartitionObjectMap);
        }
    }

    /*
    * 获取kafka每个分区消费到的offset,以便继续消费
    * */
    public static Map<TopicAndPartition, Long> getConsumerOffsets(KafkaCluster kafkaCluster, String groupId, Set<String> topicSet) {
        // 将Java的Set结构转换为Scala的mutable.Set结构
        scala.collection.mutable.Set<String> mutableTopics = JavaConversions.asScalaSet(topicSet);
        // 将Scala的mutable.Set结构转换为immutable.Set结构
        scala.collection.immutable.Set<String> immutableTopics = mutableTopics.toSet();
        // 根据传入的分区，获取TopicAndPartition形式的返回数据
        scala.collection.immutable.Set<TopicAndPartition> topicAndPartitionSet2 = (scala.collection.immutable.Set<TopicAndPartition>)
                kafkaCluster.getPartitions(immutableTopics).right().get();


        // 创建用于存储offset数据的HashMap
        // (TopicAndPartition, Offset)
        Map<TopicAndPartition, Long> consumerOffsetsLong = new HashMap();

        // kafkaCluster.getConsumerOffsets：通过kafkaCluster的getConsumerOffsets方法获取指定消费者组合，指定主题分区的offset
        // 如果返回Left，代表获取失败，Zookeeper中不存在对应的offset，因此HashMap中对应的offset应该设置为0
        if (kafkaCluster.getConsumerOffsets(groupId, topicAndPartitionSet2).isLeft()) {

            // 将Scala的Set结构转换为Java的Set结构
            Set<TopicAndPartition> topicAndPartitionSet1 = JavaConversions.setAsJavaSet(topicAndPartitionSet2);

            // 由于没有保存offset（该group首次消费时）, 各个partition offset 默认为0
            for (TopicAndPartition topicAndPartition : topicAndPartitionSet1) {
                consumerOffsetsLong.put(topicAndPartition, 0L);
            }
        } else {
            // offset已存在, 获取Zookeeper上的offset
            // 获取到的结构为Scala的Map结构
            scala.collection.immutable.Map<TopicAndPartition, Object> consumerOffsetsTemp =
                    (scala.collection.immutable.Map<TopicAndPartition, Object>)
                            kafkaCluster.getConsumerOffsets(groupId, topicAndPartitionSet2).right().get();

            // 将Scala的Map结构转换为Java的Map结构
            Map<TopicAndPartition, Object> consumerOffsets = JavaConversions.mapAsJavaMap(consumerOffsetsTemp);

            // 将Scala的Set结构转换为Java的Set结构
            Set<TopicAndPartition> topicAndPartitionSet1 = JavaConversions.setAsJavaSet(topicAndPartitionSet2);

            // 将offset加入到consumerOffsetsLong的对应项
            for (TopicAndPartition topicAndPartition : topicAndPartitionSet1) {
                Long offset = (Long) consumerOffsets.get(topicAndPartition);
                consumerOffsetsLong.put(topicAndPartition, offset);
            }
        }

        return consumerOffsetsLong;
    }

    public static KafkaCluster getKafkaCluster(Map<String, String> kafkaParams) {
        // 将Java的HashMap转化为Scala的mutable.Map
        scala.collection.mutable.Map<String, String> testMap = JavaConversions.mapAsScalaMap(kafkaParams);
        // 将Scala的mutable.Map转化为imutable.Map
        scala.collection.immutable.Map<String, String> scalaKafkaParam =
                testMap.toMap(new Predef.$less$colon$less<Tuple2<String, String>, Tuple2<String, String>>() {
                    public Tuple2<String, String> apply(Tuple2<String, String> v1) {
                        return v1;
                    }
                });

        // 由于KafkaCluster的创建需要传入Scala.HashMap类型的参数，因此要进行上述的转换
        // 将immutable.Map类型的Kafka参数传入构造器，创建KafkaCluster
        return new KafkaCluster(scalaKafkaParam);
    }

}
