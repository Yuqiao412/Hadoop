package com.example.hadoopdemo.mapmatching.utils;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class FileUtils {
    public static String readJsonFile(String fileName) {
        try {
            File jsonFile = new File(fileName);
            FileReader fileReader = new FileReader(jsonFile);
            Reader reader = new InputStreamReader(new FileInputStream(jsonFile), StandardCharsets.UTF_8);
            int count;
            StringBuilder sb = new StringBuilder();
            char[] buffer  = new char[2048];
            while ((count = reader.read(buffer)) != -1) {
                sb.append(new String(buffer,0, count));
            }
            fileReader.close();
            reader.close();
            return sb.toString();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
    public static void writeJsonFile(String fileName, String content) {
        try {
            File jsonFile = new File(fileName);
            if(!jsonFile.getParentFile().exists() && !jsonFile.mkdirs()){
                return;
            }
            if(!jsonFile.exists() && !jsonFile.createNewFile()){
                return;
            }
            FileWriter fileReader = new FileWriter(jsonFile);
            Writer writer = new OutputStreamWriter(new FileOutputStream(jsonFile), StandardCharsets.UTF_8);
            writer.append(content);
            fileReader.close();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
