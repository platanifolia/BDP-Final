package CharacterSameScene;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.StringTokenizer;

public class CharacterMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException,
            InterruptedException {

        ArrayList<String> name_token = new ArrayList<>();
        StringTokenizer string_stream = new StringTokenizer(value.toString());

        while (string_stream.hasMoreTokens()) {
            String temp = string_stream.nextToken();

            if (Objects.equals(temp, "唐三藏") ||
                    Objects.equals(temp, "陈玄奘") ||
                    Objects.equals(temp, "玄奘") ||
                    Objects.equals(temp, "唐长老") ||
                    Objects.equals(temp, "金蝉子") ||
                    Objects.equals(temp, "旃檀功德佛") ||
                    Objects.equals(temp, "江流儿") ||
                    Objects.equals(temp, "江流")) {
                temp = "唐三藏";
            } else if (Objects.equals(temp, "悟空") ||
                    Objects.equals(temp, "齐天大圣") ||
                    Objects.equals(temp, "美猴王") ||
                    Objects.equals(temp, "猴王") ||
                    Objects.equals(temp, "斗战胜佛") ||
                    Objects.equals(temp, "孙行者") ||
                    Objects.equals(temp, "心猿") ||
                    Objects.equals(temp, "金公")) {
                temp = "孙悟空";
            } else if (Objects.equals(temp, "猪悟能") ||
                    Objects.equals(temp, "悟能") ||
                    Objects.equals(temp, "八戒") ||
                    Objects.equals(temp, "猪刚鬣") ||
                    Objects.equals(temp, "老猪") ||
                    Objects.equals(temp, "净坛使者") ||
                    Objects.equals(temp, "天蓬元帅") ||
                    Objects.equals(temp, "木母")) {
                temp = "猪八戒";
            } else if (Objects.equals(temp, "沙和尚") ||
                    Objects.equals(temp, "沙悟净") ||
                    Objects.equals(temp, "悟净") ||
                    Objects.equals(temp, "金身罗汉") ||
                    Objects.equals(temp, "卷帘大将") ||
                    Objects.equals(temp, "刀圭")) {
                temp = "沙僧";
            } else if (Objects.equals(temp, "小白龙") ||
                    Objects.equals(temp, "白马") ||
                    Objects.equals(temp, "八部天龙马")) {
                temp = "白龙马";
            } else if (Objects.equals(temp, "如来")) {
                temp = "如来佛祖";
            } else if (Objects.equals(temp, "观音") ||
                    Objects.equals(temp, "观世音菩萨") ||
                    Objects.equals(temp, "观世音")) {
                temp = "观音菩萨";
            } else if (Objects.equals(temp, "玉皇大帝")) {
                temp = "玉帝";
            }
            if (!name_token.contains(temp))
                name_token.add(temp);
        }

        int array_length = name_token.size();
        for (int i = 0; i < array_length; i++) {
            for (int j = 0; j < array_length; j++) {
                if (i != j) {
                    context.write(
                            new Text("(" + name_token.get(i) + "," + name_token.get(j) + ")"),
                            one);
                }
            }
        }
    }
}
