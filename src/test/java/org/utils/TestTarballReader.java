package org.utils;

import static org.junit.Assert.*;

import java.util.Calendar;
import java.util.TimeZone;

import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 * TestTarballReader.
 *
 * Under Apache License 2.0 
 * 
 * @author pgrandjean
 * @date 27 Jun 2014
 * @since 1.6.x
 */
public class TestTarballReader {

    @Test
    public void testSimpleTarball() throws Exception {
        TarballReader tarballReader = new TarballReader("/test.1.tar.gz");

        int numberOfFiles = 0;
        while (tarballReader.nextKeyValue()) numberOfFiles++;

        assertEquals(10, numberOfFiles);
        
        tarballReader.close();
    }

    @Test
    public void testExtractedFileSizes() throws Exception {
        TarballReader tarballReader = new TarballReader("/test.1.tar.gz");

        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        int[] sizes = { 8069, 6112, 10638, 8258, 9280, 10080, 9669, 11767, 13255, 13875 };
        int i = 0;
        
        while (tarballReader.nextKeyValue()) {
            TarballEntry key = tarballReader.getCurrentKey();
            Text value = tarballReader.getCurrentValue();
            
            assertEquals("/test.1.tar.gz", key.getTarball());
            assertTrue(key.getEntry().endsWith(".txt"));
            assertEquals(sizes[i++], value.getLength());
            
            cal.setTimeInMillis(key.getModTime());
            assertEquals(2014, cal.get(Calendar.YEAR));
            assertEquals(5, cal.get(Calendar.MONTH));
            assertEquals(27, cal.get(Calendar.DAY_OF_MONTH));
            assertEquals(9, cal.get(Calendar.HOUR_OF_DAY));
            assertEquals(6, cal.get(Calendar.MINUTE));
            assertEquals(23, cal.get(Calendar.SECOND));
        }
        
        tarballReader.close();
    }

    @Test
    public void testTarballWithFolders() throws Exception {
        TarballReader tarballReader = new TarballReader("/test.2.tar.gz");
        
        int numberOfFiles = 0;
        while (tarballReader.nextKeyValue()) numberOfFiles++;

        assertEquals(12, numberOfFiles);
        
        tarballReader.close();
    }

    @Test
    public void testTarballWithFoldersFileSizes() throws Exception {
        TarballReader tarballReader = new TarballReader("/test.2.tar.gz");

        int[] sizes = { 8069, 6112, 10638, 8258, 9280, 10080, 9669, 11767, 13255, 13875, 8069, 6112 };
        int i = 0;
        while (tarballReader.nextKeyValue()) {
            Text value = tarballReader.getCurrentValue();
            assertEquals(sizes[i++], value.getLength());
        }
        
        tarballReader.close();
    }
}
