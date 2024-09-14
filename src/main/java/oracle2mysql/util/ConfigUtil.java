package oracle2mysql.util;

import cn.hutool.setting.Setting;

public class ConfigUtil {
    static Setting setting = new Setting(System.getProperty("user.dir") + "/config.ini");

    public static String getConfig(String key) {
        return setting.getStr(key);
    }
}
