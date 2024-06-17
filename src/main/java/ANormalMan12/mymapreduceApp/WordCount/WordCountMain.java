package ANormalMan12.mymapreduceApp.WordCount;

import ANormalMan12.mymapreduce.client.ClientClass;

public class WordCountMain {
    public static void main(String[] args) throws Exception {
        //TODO：需要实现将输入文件和jar拷贝到缓存区目录的操作。
        if(args.length!=3){
            System.out.println("非法数量的参数，无法启动");
        }
        String clientIPaddress=args[0];
        String inputFilePath=args[1];
        String jarPath = args[2];
        String mapperClassName= WordCountMapper.class.getName();
        String reduceClassName= WordCountReducer.class.getName();
        int M=5;
        int R=5;
        ClientClass.runClient(clientIPaddress,inputFilePath,jarPath,mapperClassName,reduceClassName,M,R);
    }
}
