public class DurationThread extends Thread {

    public static void Refresh(){

        //create 255 Thread using for loop
        for (int x = 0; x < 256; x++) {
            // Create Thread class
            DurationThread temp = new DurationThread();
            temp.start();
            try {
                temp.join(10);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
