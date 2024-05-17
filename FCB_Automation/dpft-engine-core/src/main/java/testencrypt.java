import com.ibm.util.EncryptUtil;

public class testencrypt {

	public static void main(String[] args) {
		System.out.println(EncryptUtil.IdEncrypt("S123456789"));
		System.out.println(EncryptUtil.IdDecrypt("RZwQw6p7/dbA3JJ94ZwjqBkfpw2yxqVT"));
	}
}
