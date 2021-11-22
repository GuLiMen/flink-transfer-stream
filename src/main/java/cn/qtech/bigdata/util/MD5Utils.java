package cn.qtech.bigdata.util;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5Utils {
    public static String StringToMD5(String plainText) {
        byte[] secureBytes = null;
        try {
            // 生成一个MD5加密计算摘要 计算md5函数
            secureBytes = MessageDigest.getInstance("md5").digest(plainText.getBytes());

        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("" +
                    "没有这个md5算法！");
        }

        String md5code = new BigInteger(1, secureBytes).toString(16);
        for (int i = 0; i < 32 - md5code.length(); i++) {
            md5code = "0" + md5code;
        }
        return md5code;
    }

/*    public static void main(String[] args) {
        String fsda = MD5Utils.StringToMD5("");
        System.out.println(fsda);
    }*/
}
