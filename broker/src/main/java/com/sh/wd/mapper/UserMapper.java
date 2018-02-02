package com.sh.wd.mapper;

import com.sh.wd.bean.MessageLogEntity;
import org.apache.ibatis.annotations.Param;

/**
 * Created by admin on 2017/8/29.
 */
public interface UserMapper {

    int userLogin(@Param("user") String user, @Param("password") String password);
    int insertMessageLog(@Param("message") MessageLogEntity message);
    boolean updateMessageState(@Param("messageId") int id, @Param("state") int state);
    void updataUserState(@Param("account") String account, @Param("state") int state);
    boolean addUserState(@Param("account") String account, @Param("time") String time, @Param("state") int state);
//    List<CustomerEntity> onLineCustomer();
}
