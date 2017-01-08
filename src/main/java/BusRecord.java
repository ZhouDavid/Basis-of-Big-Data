/**
 * Created by Administrator on 2017/1/4.
 */
public class BusRecord {
    String raw_time = new String();
    String md5_mac = new String();
    Double lng = new Double(0);
    Double lat = new Double(0);
    Double velo = new Double(0);
    Double angle = new Double(0);
    Long time_stamp = new Long(0);
    public BusRecord(String[]args)throws Exception{
        if(args.length!=10){
            System.err.println("record error!");
            return;
        }
        raw_time = args[0];
        md5_mac = args[2];
        lng = Double.parseDouble(args[4]);
        lat = Double.parseDouble(args[5]);
        velo = Double.parseDouble(args[6]);
        angle = Double.parseDouble(args[7]);
        time_stamp = Long.parseLong(args[8]);
    }

}
