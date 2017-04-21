package com.idscorporation.service;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

/**
 * Created by v.carlomusto on 11/04/2017.
 */
@Service
public class converter {

    private ClassLoader classLoader = getClass().getClassLoader();
    private String[] metrics = {"EC20min", "EC1h20min", "OCC5min"};
    private Long[] metricsTimestamp = {new Long(1200000), new Long(4800000), new Long(300000)};
    private HashMap<String, Long> metricMap = new HashMap<>();
    private String startDate = "2017-03-22T00:00:00Z";
    private HashMap<String, String> streamMap = new HashMap<>();
    private HashMap<String, String> parentsectormap = new HashMap<>();

    private void setupSectorMapping() {
        try {
            Path path = Paths.get(classLoader.getResource("input/parentsector.map").toURI());
            Files.readAllLines(path).stream().filter(x -> x.length() > 0).forEach(psec -> Arrays.stream((psec.split(":")
                    [1]).split(",")).forEach(sec -> parentsectormap.put(sec, (psec.split(":")[0]))));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

    }

    private void setupStreamMapping() {
        try {
            Path path = Paths.get(classLoader.getResource("input/stream_mapping.map").toURI());
            Files.readAllLines(path).stream().filter(x -> x.length() > 0).forEach(x -> streamMap.put(x.split(",")[0], x
                    .split(",")[1]));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    private void setupMetrics() {
        try {
            Path path = Paths.get(classLoader.getResource("input/metrics.csv").toURI());
            String metricslist = Files.readAllLines(path).get(0);
            String metricsTimestampList = Files.readAllLines(path).get(1);
            List<Long> ts = new ArrayList<Long>();
            metrics = metricslist.split(",");
            Arrays.stream(metricsTimestampList.split(",")).forEach(x -> ts.add(Long.parseLong(x)));
            metricsTimestamp = new Long[ts.size()];
            metricsTimestamp = ts.toArray(metricsTimestamp);
            for (int i = 0; i < metrics.length; i++) {
                metricMap.put(metrics[i], metricsTimestamp[i]);
            }
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @PostConstruct
    public String convert() {
        setupStreamMapping();
        setupMetrics();
        setupSectorMapping();
        File file = new File(classLoader.getResource("input/input.xlsx").getFile());
        HashMap<String, MetricStructure> data = new HashMap<>();
        JSONObject json = null;
        Workbook workbook = null;
        List rows = new ArrayList();
        try {
            FileInputStream inp = new FileInputStream(file);
            workbook = WorkbookFactory.create(inp);

            // Get the first Sheet.
            for (Sheet sheet : workbook) {


                // Iterate through the rows.
                for (Iterator<Row> rowsIT = sheet.rowIterator(); rowsIT.hasNext(); ) {
                    Row row = rowsIT.next();

                    // Iterate through the cells.
                    List cells = new ArrayList();
                    for (Iterator<Cell> cellsIT = row.cellIterator(); cellsIT.hasNext(); ) {
                        Cell cell = cellsIT.next();
                        cells.add(getStringValue(cell));
                    }
                    rows.add(cells);
                }
                data = createStructure(rows, data, sheet.getSheetName());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InvalidFormatException e) {
            e.printStackTrace();
        }

        JSONObject jsonObject = createJson(data);
        writeJson(jsonObject);
        // Get the JSON text.
        return jsonObject.toString();
    }

    private void writeJson(JSONObject jsonObject) {
        try (FileWriter file = new FileWriter("output.json")) {
            file.write(jsonObject.toString());
            System.out.println("Successfully Copied JSON Object to File...");
            System.out.println("\nJSON Object: " + jsonObject.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private HashMap<String, MetricStructure> createStructure(List rows, HashMap<String, MetricStructure> data, String
            sector) {
        List<String> metricsList = Arrays.asList(metrics);
        String currentMetric = "";
        for (Object row : rows) {
            List cells = (List) row;
            if (metricsList.contains(cells.get(0))) {
                currentMetric = cells.get(0).toString();
//                data.put(currentMetric, generateRandomValue(data.get(currentMetric), currentMetric, sector, cells.size()));
            } else if (streamMap.keySet().contains(cells.get(0))) {
                List collect = (List) cells.stream().filter(a -> false == a.equals(cells.get(0))).collect(Collectors
                        .toList());
                data.put(currentMetric, fillStream(streamMap.get(cells.get(0)), data.get(currentMetric), sector, collect,
                        currentMetric));
            }
        }
        return data;
    }

    private MetricStructure generateRandomValue(MetricStructure data, String currentMetric, String sectorName, int size) {
        MetricStructure result = data;
        String[] streams = {"Intruders", "Complexity"};
        for (String currentStream : streams) {
            List randomValues = new Random().ints(size, 0, 4).boxed().collect(Collectors.toList());
            result = fillStream(currentStream, result, sectorName, randomValues, currentMetric);
        }
        return result;
    }

    private MetricStructure fillStream(String streamKey, MetricStructure data, String sectorName, List collect, String
            currentMetric) {
        if (data == null) {
            data = new MetricStructure();
        }
        if (data.sectors == null) {
            data.sectors = new HashMap<>();
        }
        Sector mysector = data.sectors.get(sectorName);
        if (null == mysector) {
            mysector = new Sector();
        }
        mysector.sectorName = sectorName;
        mysector.parentSector = parentsectormap.get(sectorName);
        if (mysector.streams == null) {
            mysector.streams = new HashMap<>();
        }
        mysector.streams.put(streamKey, collect);
        data.sectors.put(sectorName, mysector);
        data.startDate = this.startDate;
        Long timestamp = metricMap.get(currentMetric);
        data.timestempstep = String.valueOf(timestamp);
        return data;
    }


    public JSONObject createJson(HashMap<String, MetricStructure> data) {
        JSONObject json = new JSONObject();
        try {
            for (Map.Entry<String, MetricStructure> metricStructureEntry : data.entrySet()) {
                JSONObject jsonMetric = createJsonMetric(json, metricStructureEntry.getValue());
                json.put(metricStructureEntry.getKey(), jsonMetric);
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return json;
    }

    private JSONObject createJsonMetric(JSONObject json, MetricStructure value) throws JSONException {
        JSONObject jmetric = new JSONObject();
        jmetric.put("startdate", value.startDate);
        jmetric.put("timestempstep", value.timestempstep);
        JSONArray sectors = createSectorArrayJson(value.sectors);
        jmetric.put("sectors", sectors);
        return jmetric;
    }

    private JSONArray createSectorArrayJson(HashMap<String, Sector> sectors) throws JSONException {
        JSONArray jsonSectors = new JSONArray();
        for (Map.Entry<String, Sector> stringSectorEntry : sectors.entrySet()) {
            JSONObject sector = new JSONObject();
            Sector mysect = stringSectorEntry.getValue();
            sector.put("sectname", mysect.sectorName);
            sector.put("parentsect", mysect.parentSector);
            JSONArray jstream = new JSONArray();
            jstream = craeteJasonStream(mysect.streams);
            sector.put("streams", jstream);
            jsonSectors.put(sector);
        }
        return jsonSectors;
    }

    private JSONArray craeteJasonStream(HashMap<String, List> streams) throws JSONException {
        JSONArray jsonStreams = new JSONArray();
        for (Map.Entry<String, List> stringListEntry : streams.entrySet()) {
            JSONObject stream = new JSONObject();
            stream.put("key", stringListEntry.getKey());
            List value = stringListEntry.getValue();
            JSONArray jsonArray = null;
            if (value.get(0).toString().contains(".")) {
                double[] values = convertDoubleArray(value);
                jsonArray = new JSONArray(values);
            }
            else
            {
                int[] values = convertIntArray(value);
                jsonArray = new JSONArray(values);
            }

            stream.put("value", jsonArray);
            jsonStreams.put(stream);
        }
        return jsonStreams;
    }

    private double[] convertDoubleArray(List value) {
        double[] values = new double[value.size()];
        for (int i = 0; i < value.size(); i++) {
            try {
                values[i] = Double.valueOf(value.get(i).toString());
            }
            catch (NumberFormatException ex)
            {
                System.err.println(ex.getMessage());
                ex.printStackTrace();
            }
        }
        return values;
    }

    private int[] convertIntArray(List value) {
        int[] values = new int[value.size()];
        for (int i = 0; i < value.size(); i++) {
            try {
                values[i] = Integer.valueOf(value.get(i).toString());
            }
            catch (NumberFormatException ex)
            {
                System.err.println(ex.getMessage());
                ex.printStackTrace();
            }
        }
        return values;
    }

    private String getStringValue(Cell cell) {
        String result = "";
        switch (cell.getCellTypeEnum()) {
            case STRING:
                result = cell.getStringCellValue();
                break;
            case NUMERIC:
            case FORMULA:
                double numericCellValue = cell.getNumericCellValue();
                if (numericCellValue == (int)numericCellValue) {
                    result = String.valueOf((int)numericCellValue);
                }
                else {
                    result = String.valueOf(cell.getNumericCellValue());
                }
                break;
            default:
                result = "";
                System.err.println("Cell type not handled.");
                break;
        }
        return result;
    }
}
