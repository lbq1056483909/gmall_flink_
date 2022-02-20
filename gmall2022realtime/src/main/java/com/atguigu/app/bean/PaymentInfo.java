package com.atguigu.app.bean;
/**
 * @ClassName PaymentInfo
 * @Description TODO
 * @Author 10564
 * @Date 2022/2/16 1:16
 * @Version 1.0
 */
import lombok.Data;

import java.math.BigDecimal;

@Data
public class PaymentInfo {
    Long id;
    Long order_id;
    Long user_id;
    BigDecimal total_amount;
    String subject;
    String payment_type;
    String create_time;
    String callback_time;
}

