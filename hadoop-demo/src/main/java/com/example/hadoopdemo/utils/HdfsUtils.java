package com.example.hadoopdemo.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

@Component
public class HdfsUtils {

    @Value("${hadoop.hdfs}")
    private String hdfs;
    @Value("${hadoop.user}")
    private String user;

    public FileSystem getFileSystem() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        return FileSystem.get(new URI(hdfs), conf, user);
    }
    public void removeDir(String relativePath){
        Path path = new Path(relativePath);
        FileSystem fs = null;
        try {
            fs = getFileSystem();
            fs.delete(path,true);
            fs.close();
        } catch (URISyntaxException | IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
    public void mkdir(String relativePath) {
        Path path = new Path(relativePath);
        FileSystem fs = null;
        try {
            fs = getFileSystem();
            fs.mkdirs(path);
            fs.close();
        } catch (URISyntaxException | IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
    public boolean dirExist(String relativePath){
        FileSystem fs = null;
        try {
            fs = getFileSystem();
            // 第一个参数是本地windows下的文件路径 第二个参数是hdfs的文件路径
            boolean exist = fs.exists(new Path(relativePath));
            // 关闭
            fs.close();
            return exist;
        } catch (URISyntaxException | IOException | InterruptedException e) {
            e.printStackTrace();
            return false;
        }
    }
    public void upload(String localFilePath,String relativePath){
        FileSystem fs = null;
        try {
            fs = getFileSystem();
            // 第一个参数是本地windows下的文件路径 第二个参数是hdfs的文件路径
            fs.copyFromLocalFile(new Path(localFilePath), new Path(relativePath));
            // 关闭
            fs.close();
        } catch (URISyntaxException | IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
    public void download(String relativePath, String localFilePath){
        FileSystem fs = null;
        try {
            fs = getFileSystem();
            fs.copyToLocalFile(new Path(relativePath), new Path(localFilePath));
            fs.close();
        } catch (URISyntaxException | IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
    public void uploadDir(String localDir,String relativeBasePath){
        File local = new File(localDir);
        if(!dirExist(relativeBasePath)){
            mkdir(relativeBasePath);
        }
        if(local.isDirectory()){
            for(File file: Objects.requireNonNull(local.listFiles())){
                if(file.isDirectory()){
                    uploadDir(file.getAbsolutePath(),relativeBasePath+"/"+file.getName());
                }else{
                    upload(file.getAbsolutePath(),relativeBasePath);
                }
            }
        }else{
            upload(localDir,relativeBasePath);
        }
    }
    public String getJson(String txtFilePath){
        StringBuilder buffer = new StringBuilder();
        FSDataInputStream fsr = null;
        BufferedReader bufferedReader = null;
        String lineTxt = null;
        try {
            FileSystem fs = getFileSystem();
            fsr = fs.open(new Path(txtFilePath));
            bufferedReader = new BufferedReader(new InputStreamReader(fsr));
            while ((lineTxt = bufferedReader.readLine()) != null) {
                buffer.append(lineTxt);

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (bufferedReader != null) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return buffer.toString();
    }

    public String getHdfs() {
        return hdfs;
    }

    public void setHdfs(String hdfs) {
        this.hdfs = hdfs;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }
}
