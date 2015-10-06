import de.zib.scalaris.*;

public class Main {

    public static void main(String[] args) throws ConnectionException, AbortException, NotFoundException {
        TransactionSingleOp transactionSingleOp = new TransactionSingleOp();
        transactionSingleOp.write("keyA", "valueAA");
        ErlangValue value = transactionSingleOp.read("keyA");
        System.out.println(value.stringValue());
    }
}
