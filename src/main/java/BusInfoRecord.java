import java.math.BigInteger;
import java.security.MessageDigest;

/**
 * Created by Administrator on 2017/1/8.
 */
public class BusInfoRecord {
    String raw_mac = new String(); //原始mac
    String time = new String();  //登记时间
    String number_plates = new String(); //车牌号
    String busName = new String(); //公交车名
    String brand = new String(); //品牌
    String md5_mac = new String();
    public BusInfoRecord(String attrs[])throws Exception{
        raw_mac = attrs[0];
        time = attrs[1];
        number_plates = attrs[2];
        brand = attrs[3];
        busName = attrs[4];
        EncodeByMd5();
    }
    public void EncodeByMd5()throws Exception{
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        md5.update(raw_mac.getBytes());
        md5_mac = new BigInteger(1,md5.digest()).toString(16);
    }
}
