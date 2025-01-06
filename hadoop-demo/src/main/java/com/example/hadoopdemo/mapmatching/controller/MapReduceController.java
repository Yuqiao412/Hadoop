package com.example.hadoopdemo.mapmatching.controller;

import com.example.hadoopdemo.bean.Result;
import com.example.hadoopdemo.mapmatching.mapreduce.TrajectoryJob;
import com.example.hadoopdemo.utils.HdfsUtils;
import org.apache.hadoop.mapreduce.JobStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;

@Controller
@RequestMapping("/mapred")
public class MapReduceController {
    private final TrajectoryJob job;
    private final HdfsUtils hdfsUtils;

    public MapReduceController(TrajectoryJob job, HdfsUtils hdfsUtils) {
        this.job = job;
        this.hdfsUtils = hdfsUtils;
    }

//    @RequestMapping("status")
//    @ResponseBody
//    public String getStatus(){
//        try{
//            String value = job.getJob().toString();
//            return new Result<>(true,value,null).toString();
//        }catch (Exception e){
//            return new Result<>(false, e.toString(),null).toString();
//        }
//
//    }
//    @RequestMapping("kill")
//    @ResponseBody
//    public String kill(){
//        try {
//            job.getJob().killJob();
//            return new Result<>(true,"kill success",null).toString();
//        } catch (IOException e) {
//            return new Result<>(false,"kill failure",null).toString();
//        }
//    }
    @RequestMapping("execute")
    @ResponseBody
    public String execute(){
        try {
            hdfsUtils.uploadDir("D:\\workspace\\20171204","/data");
            JobStatus status = job.execute();
            return new Result<>(true,"success",status.getState().toString()).toString();
        } catch (InterruptedException | ClassNotFoundException | IOException e) {
            return new Result<>(false,"failure",e.toString()).toString();
        }
    }
}
