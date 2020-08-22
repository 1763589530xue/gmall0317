package com.atguigu.constants;

/**
 * @author xjp
 * @create 2020-08-15 9:11
 */
public class GmallConstants {
    public static final String GMALL_TOPIC_START = "Topic_Start";

    public static final String GMALL_TOPIC_EVENT = "Topic_Event";

    //订单主题
    public static final String GMALL_TOPIC_ORDER_INFO = "TOPIC_ORDER_INFO";

    //订单详情主题
    public static final String GMALL_TOPIC_ORDER_DETAIL = "TOPIC_ORDER_DETAIL";

    //用户信息主题
    public static final String GMALL_TOPIC_USER_INFO = "TOPIC_USER_INFO";

    //ES中预警日志索引前缀(该前缀需要与kibana中的模板名保持一致)
    public static final String GMALL_ES_ALERT_INFO_PRE = "gmall_coupon_alert";

    //ES中销售明细索引前缀(该前缀需要与kibana中的模板名保持一致)
    public static final String GMALL_ES_SALE_DETAIL_PRE = "gmall200317_sale_detail";

}
