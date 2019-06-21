package edu.buffalo.cse.cse486586.simpledht;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.LinkedList;
import java.util.List;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    private static final String TAG = SimpleDhtProvider.class.getSimpleName();
    private static int SERVER_PORT = 10000;
    private static String myId = null;
    private static String predecessorPort = null;
    private static String successorPort = null;
    private static String predecessor_emulator = null;
    private static String successor_emulator = null;
    private static String globalQueryString = null;
    private static String deleteString = null;
    private boolean deleteFlag = false;
    private boolean setFlag = false;

    private List<String> files = new LinkedList<String>();
    List<NodeTask> listOfNodes = new ArrayList<NodeTask>();

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub

        Context context = getContext();

        try {
            if(predecessor_emulator == null || successor_emulator == null || myId == null){
                if(selection.equals("@") || selection.equals("*")){
                    for(String f : files){
                        context.deleteFile(f);
                        files.remove(f);
                    }
                } else {
                    context.deleteFile(selection);

                    for(String f : files) {
                        if (f.equals(selection)) {
                            files.remove(f);
                        }
                    }
                }
            } else if(predecessor_emulator != null && successor_emulator != null && myId != null){
                if(selection.equals("@")){
                    for(String f : files){
                        context.deleteFile(f);
                        files.remove(f);
                    }
                } else if(selection.equals("*")){
                    deleteString = "DeleteAll,";
                    deleteFlag = true;

                    for(String f : files){
                        context.deleteFile(f);
                        files.remove(f);
                    }

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(successorPort));
                    DataInputStream inputMessage = new DataInputStream(socket.getInputStream());
                    DataOutputStream outputMessage = new DataOutputStream(socket.getOutputStream());

                    outputMessage.writeUTF(deleteString);

                    deleteString = inputMessage.readUTF();

                    outputMessage.flush();
                    outputMessage.close();
                    inputMessage.close();

                    deleteString = null;
                    deleteFlag = false;
                } else {
                    String predecessorHash = null;
                    String id = genHash(selection);
                    predecessorHash = genHash(predecessor_emulator);

                    if (lookUp(id, predecessorHash)) {
                        context.deleteFile(selection);

                        for (String f : files) {
                            if (f.equals(selection))
                                files.remove(f);
                        }
                    } else {
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "DeleteFile", selection, successorPort);
                    }
                }
            }
        } catch (NoSuchAlgorithmException nsa){
            Log.e(TAG, "No Such Algorithm Exceptions : " + nsa.getMessage());
        } catch (Exception e){
            Log.e(TAG, "Exception while deleting" + e.getMessage());
        }
        return 0;
    }

    public String deleteAll(String delString){
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(successorPort));
            DataInputStream inputMessage = new DataInputStream(socket.getInputStream());
            DataOutputStream outputMessage = new DataOutputStream(socket.getOutputStream());

            outputMessage.writeUTF(delString);

            delString = inputMessage.readUTF();

            outputMessage.flush();
            inputMessage.close();
            outputMessage.close();

        } catch (IOException e) {
            Log.i(TAG, "IO Exception during query all");
        }

        return delString;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean lookUp(String id, String predecessorHash){
        if(myId.compareTo(id) >= 0 && predecessorHash.compareTo(id) < 0)
            return true;
        else if(myId.compareTo(id) < 0 && predecessorHash.compareTo(id) < 0 && predecessorHash.compareTo(myId) > 0)
            return true;
        else if(myId.compareTo(id) > 0 && predecessorHash.compareTo(id) > 0 && predecessorHash.compareTo(myId) > 0)
            return true;
        else
            return false;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        String fileKey = values.getAsString("key");
        String fileValue = values.getAsString("value");

        //Reference: https://developer.android.com/reference/android/view/View.html#getContext()
        Context context = getContext();

        try{
            if(predecessor_emulator == null || successor_emulator == null || myId == null){
                files.add(fileKey);
                DataOutputStream outputStream = new DataOutputStream(context.openFileOutput(fileKey, Context.MODE_PRIVATE));
                outputStream.writeUTF(fileValue);
                outputStream.flush();
                outputStream.close();
            } else if(predecessor_emulator != null && successor_emulator != null && myId != null){
                String predecessorHash = null;
                String id = genHash(fileKey);
                predecessorHash = genHash(predecessor_emulator);

                if(lookUp(id, predecessorHash)){
                    files.add(fileKey);
                    DataOutputStream outputStream = new DataOutputStream(context.openFileOutput(fileKey, Context.MODE_PRIVATE));
                    outputStream.writeUTF(fileValue);
                    outputStream.flush();
                    outputStream.close();
                } else {
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "InsertFile", fileKey, fileValue, successorPort);
                }
            }
        } catch (NoSuchAlgorithmException nsa) {
            Log.e(TAG, "No Such Algorithm Exceptions : " + nsa.getMessage());
        } catch (FileNotFoundException e) {
            Log.e(TAG, "File not found exception while inserting values : "+e.getMessage());
        } catch (IOException e) {
            Log.e(TAG, "IO exception while inserting values : "+e.getMessage());
        } catch (Exception e){
            Log.e(TAG, "Can't insert values : "+e.getMessage());
        }

        Log.v("insert", values.toString());
        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        Context con = getContext();

        //Referred from PA2B
        TelephonyManager tel = (TelephonyManager) con.getSystemService(con.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf(Integer.parseInt(portStr) * 2);

        try{
            myId = genHash(portStr);

            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        } catch (IOException io){
            Log.e(TAG, "Can't create ServerSocket");
        } catch (NoSuchAlgorithmException nsa){
            Log.e(TAG, "NoSuchAlgorithm exception in ServerSocket");
        }

        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "NodeJoin", myId, myPort, portStr);
        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub

        String[] strArray = new String[]{"key", "value"};
        Object[] strObject;
        MatrixCursor mCursor = new MatrixCursor(strArray);

        //Reference: https://developer.android.com/reference/android/view/View.html#getContext()
        Context context = getContext();

        try {
            if (predecessor_emulator == null || successor_emulator == null || myId == null) {
                if (selection.equals("@") || selection.equals("*")) {
                    for (String i : files) {
                        DataInputStream inputStream = new DataInputStream(context.openFileInput(i));
                        String str = inputStream.readUTF();
                        if (str != null) {
                            strObject = new Object[]{i, str};
                            mCursor.addRow(strObject);
                        }
                    }
                } else {
                    DataInputStream inputStream = new DataInputStream(context.openFileInput(selection));
                    String str = inputStream.readUTF();
                    if(str != null) {
                        strObject = new Object[]{selection, str};
                        mCursor.addRow(strObject);
                    }
                }
            } else if (predecessor_emulator != null && successor_emulator != null && myId != null) {
                if (selection.equals("@")) {
                    if(!files.isEmpty()) {
                        for (String i : files) {
                            DataInputStream inputStream = new DataInputStream(context.openFileInput(i));
                            String str = inputStream.readUTF();
                            if (str != null) {
                                strObject = new Object[]{i, str};
                                mCursor.addRow(strObject);
                            }
                        }
                    }
                } else if (selection.equals("*")) {
                    if(!files.isEmpty()) {
                        for (String i : files) {
                            DataInputStream inputStream = new DataInputStream(context.openFileInput(i));
                            String str = inputStream.readUTF();
                            if (str != null) {
                                strObject = new Object[]{i, str};
                                mCursor.addRow(strObject);
                            }
                        }
                    }

                    int count = mCursor.getCount();
                    int columnCount = mCursor.getColumnCount();
                    setFlag = true;
                    globalQueryString = "QueryStar,";

                    List<String> strC = new ArrayList<String>();

                    //Reference : https://stackoverflow.com/questions/10723770/whats-the-best-way-to-iterate-an-android-cursor
                    if (count > 0) {
                        mCursor.moveToFirst();
                        do {
                            for (int i = 0; i < columnCount; i++) {
                                strC.add(mCursor.getString(i));
                            }
                        }while (mCursor.moveToNext());
                    }

                    if(strC != null) {
                        for (String s : strC) {
                            globalQueryString += s + ",";
                        }
                    }

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(successorPort));
                    DataInputStream inputMessage = new DataInputStream(socket.getInputStream());
                    DataOutputStream outputMessage = new DataOutputStream(socket.getOutputStream());

                    outputMessage.writeUTF(globalQueryString);

                    globalQueryString = inputMessage.readUTF();

                    if(globalQueryString != null) {
                        String[] newArray = globalQueryString.split(",");

                        int i = 1;
                        while (i < newArray.length - 1) {
                            strObject = new Object[]{newArray[i], newArray[i + 1]};
                            mCursor.addRow(strObject);
                            i += 2;
                        }
                    }

                    outputMessage.flush();
                    inputMessage.close();
                    outputMessage.close();

                    //Reset values for next query
                    globalQueryString = null;
                    setFlag = false;
                } else {
                    String predecessorHash = null;
                    String id = genHash(selection);
                    predecessorHash = genHash(predecessor_emulator);

                    if (lookUp(id, predecessorHash)) {
                        DataInputStream inputStream = new DataInputStream(context.openFileInput(selection));
                        String str = inputStream.readUTF();
                        if(str != null) {
                            strObject = new Object[]{selection, str};
                            mCursor.addRow(strObject);
                        }
                    } else {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(successorPort));

                        DataInputStream inputMessage = new DataInputStream(socket.getInputStream());
                        DataOutputStream outputMessage = new DataOutputStream(socket.getOutputStream());

                        outputMessage.writeUTF("QueryForward" + "," + selection);
                        String str = inputMessage.readUTF();
                        if(str!= null && str.length() > 1) {
                            String splitMessage[] = str.split(",");
                            strObject = new Object[]{splitMessage[1], splitMessage[2]};
                            mCursor.addRow(strObject);
                        }

                        outputMessage.flush();
                        inputMessage.close();
                        outputMessage.close();
                    }
                }
            }
        } catch (NoSuchAlgorithmException nsa) {
            Log.e(TAG, "No Such Algorithm Exceptions : " + nsa.getMessage());
        } catch (FileNotFoundException e) {
            Log.e(TAG, "File not found exception while querying values : " + e.getMessage());
        } catch (IOException e) {
            Log.e(TAG, "IO exception while querying values : " + e.getMessage());
        } catch (Exception e) {
            Log.e(TAG, "Can't query values : " + e.getMessage());
        }

        Log.v("query", selection);
        return mCursor;
    }

    public String queryAll(String queryString){
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(successorPort));
            DataInputStream inputMessage = new DataInputStream(socket.getInputStream());
            DataOutputStream outputMessage = new DataOutputStream(socket.getOutputStream());

            outputMessage.writeUTF(queryString);

            queryString = inputMessage.readUTF();

            outputMessage.flush();
            inputMessage.close();
            outputMessage.close();

        } catch (IOException e) {
            Log.i(TAG, "IO Exception during query all");
        }

        return queryString;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

        //Referred from OnTestClickListener.java
        private Uri buildUri(String scheme, String authority) {
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            return uriBuilder.build();
        }

        @Override
        protected Void doInBackground(ServerSocket... serverSockets) {
            ServerSocket serverSocket = serverSockets[0];
            Log.i(TAG, "Serversocket contents : "+serverSocket);

            while (true){
                try {
                    Socket socket = serverSocket.accept();
                    DataInputStream inputMessage = new DataInputStream(socket.getInputStream());
                    DataOutputStream outputMessage = new DataOutputStream(socket.getOutputStream());

                    String message = inputMessage.readUTF();
                    String[] splitMessage = message.split(",");

                    if(splitMessage[0].contains("InsertFile")){
                        ContentValues contentValues = new ContentValues();
                        contentValues.put("key",splitMessage[1]);
                        contentValues.put("value", splitMessage[2]);
                        insert(mUri, contentValues);
                    } else if(splitMessage[0].contains("NodeJoin")){
                        NodeTask nTask = new NodeTask(splitMessage[1], null, null,splitMessage[2],splitMessage[3]);
                        listOfNodes.add(nTask);

                        if(listOfNodes.size() != 0 && listOfNodes.size() != 1) {
                            //Sort the list based on hash value of nodes
                            Collections.sort(listOfNodes);

                            //Update all the nodes in the ring with successor and predecessor values
                            UpdateAllNodes();
                        }
                    } else if(splitMessage[0].contains("MultiCast")){
                        myId = splitMessage[1];
                        predecessorPort = splitMessage[2];
                        successorPort = splitMessage[3];
                        predecessor_emulator = splitMessage[4];
                        successor_emulator = splitMessage[5];
                    } else if(splitMessage[0].contains("QueryForward")){
                        Cursor queryCursor = query(mUri, null, splitMessage[1], null, null);
                        queryCursor.moveToFirst();

                        String returnKey = queryCursor.getString(queryCursor.getColumnIndex("key"));
                        String returnValue = queryCursor.getString(queryCursor.getColumnIndex("value"));

                        outputMessage.writeUTF("QueryForward" + "," + returnKey + "," + returnValue);
                    } else if(splitMessage[0].contains("QueryStar")) {
                        if(setFlag){

                        } else {
                            List<String> strC = new ArrayList<String>();

                            globalQueryString = "QueryStar,";
                            if (splitMessage.length > 1) {
                                for (int i = 1; i < splitMessage.length; i++) {
                                    strC.add(splitMessage[i]);
                                }
                            }

                            setFlag = true;

                            String[] strArray = new String[]{"key", "value"};
                            MatrixCursor mCursor = new MatrixCursor(strArray);
                            Object[] strObject;

                            if(!files.isEmpty()) {
                                for (String i : files) {
                                    DataInputStream inputStream = new DataInputStream(getContext().openFileInput(i));
                                    String str = inputStream.readUTF();
                                    if (str != null) {
                                        strObject = new Object[]{i, str};
                                        mCursor.addRow(strObject);
                                    }
                                }
                            }

                            int count = mCursor.getCount();
                            int columnCount = mCursor.getColumnCount();

                            //Reference : https://stackoverflow.com/questions/10723770/whats-the-best-way-to-iterate-an-android-cursor
                            if (count > 0) {
                                mCursor.moveToFirst();
                                do {
                                    for (int i = 0; i < columnCount; i++) {
                                        strC.add(mCursor.getString(i));
                                    }
                                } while (mCursor.moveToNext());
                            }

                            if(strC != null) {
                                for (String s : strC) {
                                    globalQueryString += s + ",";
                                }
                            }

                            globalQueryString = queryAll(globalQueryString);
                            outputMessage.writeUTF(globalQueryString);

                            //Reset values for next query
                            setFlag = false;
                            globalQueryString = null;
                        }
                    } else if(splitMessage[0].contains("DeleteFile")){
                        delete(mUri, splitMessage[1], null);
                    } else if(splitMessage[0].contains("DeleteAll")){
                        if(deleteFlag){

                        } else {
                            deleteString = "DeleteAll,";
                            deleteFlag = true;

                            for(String f : files){
                                getContext().deleteFile(f);
                                files.remove(f);
                            }

                            deleteString = deleteAll(deleteString);
                            outputMessage.writeUTF(deleteString);

                            deleteString = null;
                            deleteFlag = false;
                        }
                    }

                    outputMessage.flush();
                    outputMessage.close();
                    socket.close();

                } catch (IOException io){
                    Log.e(TAG, "ClientTask IO Exception"+io.getMessage());
                }
            }
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            String msgToSend = msgs[0];

            try {
                if(msgToSend.contains("InsertFile")){
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[3]));
                    DataOutputStream outputMessage = new DataOutputStream(socket.getOutputStream());
                    outputMessage.writeUTF(msgToSend + "," + msgs[1] + "," + msgs[2]);
                    outputMessage.flush();
                    outputMessage.close();
                } else if(msgToSend.contains("NodeJoin")){
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt("11108"));
                    DataOutputStream outputMessage = new DataOutputStream(socket.getOutputStream());
                    outputMessage.writeUTF(msgToSend + "," + msgs[1] + "," + msgs[2] + "," + msgs[3]);
                    outputMessage.flush();
                    outputMessage.close();
                } else if(msgToSend.contains("MultiCast")){
                    String[] splitMessage = msgs[1].split(",");
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(splitMessage[3]));
                    DataOutputStream outputMessage = new DataOutputStream(socket.getOutputStream());
                    outputMessage.writeUTF(msgToSend + "," + splitMessage[0] + "," + splitMessage[1] + "," + splitMessage[2] + "," +
                            splitMessage[4] + "," + splitMessage[5]);
                    outputMessage.flush();
                    outputMessage.close();
                } else if(msgToSend.contains("DeleteFile")){
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(msgs[2]));
                    DataOutputStream outputMessage = new DataOutputStream(socket.getOutputStream());
                    outputMessage.writeUTF(msgToSend + "," + msgs[1]);
                    outputMessage.flush();
                    outputMessage.close();
                }
            } catch (IOException e) {
                    e.printStackTrace();
            }
            return null;
        }
    }

    class NodeTask implements Comparable<NodeTask>{
        String NodeHashId = null;
        NodeTask predecessorNode = null;
        NodeTask successorNode = null;
        String myPort = null;
        String myPort_emulator = null;

        NodeTask(String nodeHashId, NodeTask predecessorNode, NodeTask successorNode, String myPort, String myPort_emulator){
            this.NodeHashId = nodeHashId;
            this.predecessorNode = predecessorNode;
            this.successorNode = successorNode;
            this.myPort = myPort;
            this.myPort_emulator = myPort_emulator;
        }

        @Override
        public int compareTo(NodeTask another) {
            if(this.NodeHashId.compareTo(another.NodeHashId) > 0)
                return 1;
            else
                return -1;
        }
    }

    public void UpdateAllNodes(){
        for (int i = 0; i < listOfNodes.size(); i++) {
            if (i == listOfNodes.size() - 1) {
                listOfNodes.get(0).predecessorNode = listOfNodes.get(i);
                listOfNodes.get(i).successorNode = listOfNodes.get(0);
            } else {
                listOfNodes.get(i + 1).predecessorNode = listOfNodes.get(i);
                listOfNodes.get(i).successorNode = listOfNodes.get(i + 1);
            }
        }

        for(int i=0; i<listOfNodes.size(); i++){
            String multiString = "";
            multiString = listOfNodes.get(i).NodeHashId + "," + listOfNodes.get(i).predecessorNode.myPort + "," +
                    listOfNodes.get(i).successorNode.myPort + "," + listOfNodes.get(i).myPort + "," +
                    listOfNodes.get(i).predecessorNode.myPort_emulator + "," +
                    listOfNodes.get(i).successorNode.myPort_emulator;

            //Multicast new values to all the nodes
            MultiCast(multiString);
        }
    }

    public void MultiCast(String multiString){
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "MultiCast", multiString);
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}