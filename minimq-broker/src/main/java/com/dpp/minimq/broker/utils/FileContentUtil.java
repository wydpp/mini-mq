package com.dpp.minimq.broker.utils;

import com.alibaba.fastjson.JSON;
import com.dpp.minimq.broker.model.TopicInfoModel;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * @author dpp
 * @date 2025/5/16
 * @Description 文件读取工具
 */
public class FileContentUtil {

    public static String readFromFile(String path) {
        try (BufferedReader in = new BufferedReader(new FileReader(path))) {
            StringBuffer stb = new StringBuffer();
            while (in.ready()) {
                stb.append(in.readLine());
            }
            return stb.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void overWriteToFile(String path, String content){
        try (FileWriter fileWriter = new FileWriter(path)){
            fileWriter.write(content);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public static void main(String[] args) {
        String str = FileContentUtil.readFromFile("D:\\code\\github\\mini-mq\\broker\\config\\minimq-topic.json");
        List<TopicInfoModel> miniMqTopicModels = JSON.parseArray(str, TopicInfoModel.class);
        System.out.println(miniMqTopicModels);
    }
}
