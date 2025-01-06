package com.example.hadoopdemo;

import com.example.hadoopdemo.mapmatching.mapreduce.tj.ODJob;
import com.example.hadoopdemo.mapmatching.mapreduce.tj.TrajectoryLengthJob;
import com.example.hadoopdemo.utils.HdfsUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

@SpringBootTest
public class TianjinTest {
    @Autowired
    ODJob job;
    @Autowired
    TrajectoryLengthJob job2;
    @Autowired
    HdfsUtils hdfsUtils;

    // 代码全是盲打的 还没测试哈
    @Test
    void ODJob() throws IOException, ClassNotFoundException, InterruptedException {
        // od数据设置放在 "hdfs://192.168.1.11:9000/tianjin/od"
        // 相关参数 要调整 可点进ODJob中修改 ODMapper中也有参数要调整
        job.execute();
    }

    @Test
    void TrajectoryJob() throws InterruptedException, IOException, ClassNotFoundException {
        // 轨迹数据设置是放在 "hdfs://192.168.1.11:9000/tianjin/trajectory"
        // 相关参数 要调整 可点进TrajectoryLengthJob中修改 RecordsReducer中也有参数要调整
        job2.execute();
    }
}
