package cn.qtech.bigdata.util;

import javax.mail.Authenticator;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.List;
import java.util.Properties;

public class SendEMailWarning {
    public static boolean sendMail(List<String> recipients, String subject, String content) {
        boolean flag = false;
        try {
            final Properties props = System.getProperties();
            props.put("mail.smtp.auth", "true");
            props.put("mail.smtp.host", "123.58.177.49");
            props.put("mail.user", "bigdata.it@qtechglobal.com");
            props.put("mail.password", "qtech2020");
            props.put("mail.smtp.port", "25");
            props.put("mail.smtp.starttls.enable", "true");
            // 设置邮箱认证
            Authenticator authenticator = new Authenticator() {
                protected PasswordAuthentication getPasswordAuthentication() {
                    String userName = props.getProperty("mail.user");
                    String password = props.getProperty("mail.password");
                    return new PasswordAuthentication(userName, password);
                }
            };
            // 使用环境属性和授权信息，创建邮件会话
            Session session = Session.getInstance(props, authenticator);
            // 创建邮件消息
            MimeMessage message = new MimeMessage(session);
            // 设置发件人
            InternetAddress fromAddress = new InternetAddress(props.getProperty("mail.user"));
            message.setFrom(fromAddress);

            final int num = recipients.size();
            InternetAddress[] addresses = new InternetAddress[num];
            for (int i = 0; i < num; i++) {
                addresses[i] = new InternetAddress(recipients.get(i));
            }
            message.setRecipients(MimeMessage.RecipientType.TO, addresses);
            // 设置邮件标题
            message.setSubject(subject);
            // 设置邮件的内容体
            message.setContent(content, "text/html;charset=UTF-8");
            // 发送邮件
            Transport.send(message);
            flag = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return flag;
    }

/*
    public static void main(String[] args) {
        System.out.println("start……");
        boolean flag = send_qqmail(Arrays.asList("limeng.gu@qtechglobal.com"), "测试", "job warning");
        if (flag) {
            System.out.println("发送成功！");
        }
    }
*/
}
