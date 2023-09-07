package com.bilibili.sycpb.avid.utils;


public class Constants {
    public static String REDIS_ADDRESS = "10.155.237.19:6845,10.155.224.19:6845,10.155.229.37:6846";
    public static String PID_REDIS_ADDRESS = "10.155.220.16:6811,10.155.224.37:6808,10.155.223.37:6811,10.155.234.19:6808";
    public static int PID_REDIS_EXPIRE = 1 * 1 * 30;

    public static String PID_REDIS_CLEAN_EVEN_ADDRESS = "10.156.168.17:6846,10.155.217.37:6817,10.156.169.36:6846,10.155.236.17:6818";
    public static String MODEL_INFO_REDIS = "10.155.222.36:6866,10.155.219.37:6866,10.155.223.19:6866,10.155.220.37:6864,10.156.136.36:6816";


    public static int REDIS_EXPIRE = 14 * 24 * 3600;

    //中台Redis数据
    public static String DATACENTER_REDIS_ADDR = "10.155.226.17:6852,10.155.220.37:6852,10.155.224.17:6852";
    public static int DATACENTER_REDIS_ADDR_EXPIRE = 3 * 24 * 3600;
    // 离线特征同步Redis
    public static String FEATURE_SYNC = "10.155.228.17:6821,10.156.143.16:6821,10.156.135.37:6821,10.156.139.16:6821";
    public static int GAME_TAG_TIME = 5 * 24 * 3600;

    // 特本粉丝同步
    public static String FANS_FOLLOWER_RELATION_REDIS_ADDRESS = "10.156.134.17:6823,10.156.135.37:6823,10.156.143.36:6823";
    public static int FANS_FOLLOWER_RELATION_REDIS_EXPIRE = 3 * 24 * 3600;


    public static String CONVSERION_BASE = "10.156.168.17:6871,10.156.170.37:6871,10.156.164.37:6871";
    public static int CONVSERION_BASE_EXPIRE = 7 * 24 * 3600;

    public static String COMMENT_BASE = "10.155.227.17:6822,10.156.129.37:6822,10.156.143.36:6822";
    public static int COMMENT_BASE_EXPIRE = 1 * 24 * 3600;
}
