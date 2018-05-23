package com.sino;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.hive.ql.exec.UDF;


public class wepjudge extends UDF{


//    public static void main(String[] args) {
////        System.out.println(com.sino.WEP_split.evaluate("89"));
///   }
//    public static String evaluate(String InputValue ){
//
////        InputValue="1, 0512 0521'39 15 15'1200 1218'1416 1537,17 0617 0803;SE'0844 0854;NW'1416 1440;N'1435 1538;E S,13,";
//        String[] arr = InputValue.split("\\(|\\)|'|,|\\;| ");
//        String wep_code_bb = "89";//冰雹
//        String wep_code_xb = "39";//雪暴
//        String wep_code_df = "15";//大风
//
////        System.out.println(Arrays.toString(arr));
//        boolean isContains_bb = Arrays.asList(arr).contains(wep_code_bb);
//        boolean isContains_xb = Arrays.asList(arr).contains(wep_code_xb);
//        boolean isContains_df = Arrays.asList(arr).contains(wep_code_df);
//
//        StringBuffer wep_result=new StringBuffer("");
//
//        if(isContains_bb)   wep_result.append("bb|");
//        if(isContains_xb)   wep_result.append("xb|");
//        if(isContains_df)   wep_result.append("df|");
////        System.out.println(isContains_df);
////        System.out.println(Arrays.toString(wep_result.toString().split("\\|")));
////        System.out.println(wep_result.toString().split("\\|")[0]);
//        return wep_result.toString();
//    }

//    public static void main(String[] args) {
//
//        String  fields="1, 0512 0521'39 15 15'1200 1218'1416 1537,17 0617 0803;SE'0844 0854;NW'1416 1440;N'1435 1538;E S,13,";
//        System.out.println(evaluate(fields));
//    }

    /**
     * 根据提供的天气现象码判断是否有某种天气现象
     * @param fields 数据库字段
     * @param wepcode 需要判定的天气现象码
     * @return  如果有，返回天气现象码，如果没有，返回空
     */
    public static String evaluate(String fields,String wepcode){


        String arr[]=fields.split("\\(|\\)|'|,|\\;| ");
        boolean isContains=Arrays.asList(arr).contains(wepcode);

        if (isContains) return wepcode;
        return null;
    }

    /**
     * 用于判断是否为灾害天气
     * @param fields 数据库字段
     * @return 如果是灾害天气返回"disaster"，如果不是灾害天气返回"normal"
     */
    public static String evaluate(String fields){


        String wepcode[]=fields.split("\\(|\\)|'|,|\\;| ");
        String disaster_code[]={"17","15","89","13"}; //灾害现象码需完善,目前判断为按顺序雷暴大风冰雹闪电

        Iterator<String> it = Arrays.asList(disaster_code).iterator();
        while(it.hasNext()){
            boolean isContains=Arrays.asList(wepcode).contains(it.next());
            if (isContains) return "disaster";
        }


        return "normal";
    }

}