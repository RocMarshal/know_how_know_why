package khkw.correctness;

import khkw.correctness.functions.MapFunctionWithException;
import khkw.correctness.functions.SimpleCheckpointedSource;
import khkw.correctness.functions.Tuple3KeySelector;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * 项目名称: Apache Flink 知其然，知其所以然 - khkw.correctness.functions
 * 功能描述: 1. 直接运行程序，作业会不停的重复失败，直至退出, 观察恢复时候的offset值。
 *         2. 修改SimpleCheckpointedSource的initializeState的offset值，如果是9，19变成10，20，观察效果
 * 操作步骤:
 * <p>
 * 作者： 孙金城
 * 日期： 2020/7/13
 */
public class NonParallelCheckpointedSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(3, Time.of(2, TimeUnit.SECONDS) ));

        env.enableCheckpointing(20);

        env.addSource(new SimpleCheckpointedSource())
                .map(new MapFunctionWithException())
                .keyBy(new Tuple3KeySelector())
                .sum(1).print();

        env.execute("NonParallelCheckpointedSource");
    }
}
