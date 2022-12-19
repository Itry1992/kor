import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class DiercterBUffT {
    public static void main(String[] args) {
        ByteBuffer buffer = ByteBuffer.allocateDirect(1024);
        try {
            File file = new File("C:\\Users\\wz\\Desktop\\文档编写\\0917修改\\审核意见修改稿0917批注.rar");
            File fileOut = new File("C:\\Users\\wz\\Desktop\\文档编写\\0917修改\\审核意见修改稿0917批注_c1.rar");
            FileChannel channel = new FileInputStream(file).getChannel();
            FileChannel channelOut = new FileOutputStream(fileOut).getChannel();
            int readI = 0;
            while (readI >= 0) {
                buffer.clear();
                readI = channel.read(buffer);
                buffer.flip();
                channelOut.write(buffer);
            }

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
