import de.zib.scalaris.AbortException;
import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.NotFoundException;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.*;

public class Main {

    public static void main(String[] args) throws ConnectionException, AbortException, NotFoundException {
        System.out.println("START");

        HttpClient httpClient = HttpClientBuilder.create().build();

        HttpPost request = new HttpPost("http://localhost:8000/api/tx.yaws");
        StringEntity params = null;
        try {
            params = new StringEntity("{\"jsonrpc\": \"2.0\", \"method\": \"req_list\",\n" +
                    "\"params\": [\n" +
                    "[ { \"read\": \"keyA\" },\n" +
                    "{ \"read\": \"keyB\" } ] ],\n" +
                    "\"id\": 0 }");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        request.addHeader("content-type", "application/json");
        request.setEntity(params);

        System.out.println("READ");

        try {
            HttpResponse response = httpClient.execute(request);
            InputStream stream = response.getEntity().getContent();

            BufferedReader br = new BufferedReader(
                    new InputStreamReader((response.getEntity().getContent())));

            String output;
            System.out.println("Output from Server .... \n");
            while ((output = br.readLine()) != null) {
                System.out.println(output);
            }

            stream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
