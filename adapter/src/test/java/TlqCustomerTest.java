import com.tong.kafka.tlq.TlqHolder;
import com.tongtech.client.consumer.PullCallback;
import com.tongtech.client.consumer.PullResult;
import com.tongtech.client.consumer.PullStatus;
import com.tongtech.client.consumer.common.PullType;
import com.tongtech.client.exception.TLQBrokerException;
import com.tongtech.client.exception.TLQClientException;
import com.tongtech.client.remoting.exception.RemotingException;

public class TlqCustomerTest {
    public static void main(String[] args) {
        try {
            TlqHolder.getCustomer().pullMessage(PullType.PullContinue, -1, 4, new PullCallback() {
                @Override
                public void onSuccess(PullResult pullResult) {
                    if (pullResult.getPullStatus() == PullStatus.FOUND) {
                        System.out.println("suucees "+pullResult.getMsgFoundList());
                    }
                }

                @Override
                public void onException(Throwable throwable) {

                }
            });
        } catch (TLQClientException e) {
            throw new RuntimeException(e);
        } catch (RemotingException e) {
            throw new RuntimeException(e);
        } catch (TLQBrokerException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
