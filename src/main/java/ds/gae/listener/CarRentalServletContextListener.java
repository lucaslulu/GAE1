package ds.gae.listener;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.PathElement;

import ds.gae.CarRentalModel;
import ds.gae.entities.Car;
import ds.gae.entities.CarRentalCompany;
import ds.gae.entities.CarType;

public class CarRentalServletContextListener implements ServletContextListener {

    @Override
    public void contextInitialized(ServletContextEvent arg0) {
        // This will be invoked as part of a warming request,
        // or the first user request if no warming request was invoked.

        // check if dummy data is available, and add if necessary
        if (!isDummyDataAvailable()) {
            addDummyData();
        }
    }

    private boolean isDummyDataAvailable() {
        // If the Hertz car rental company is in the datastore, we assume the dummy data
        // is available
        return CarRentalModel.get().getAllRentalCompanyNames().contains("Hertz");

    }

    private void addDummyData() {
        loadRental("Hertz", "hertz.csv");
        loadRental("Dockx", "dockx.csv");
    }

    private void loadRental(String name, String datafile) {
        Logger.getLogger(CarRentalServletContextListener.class.getName()).log(Level.INFO, "loading {0} from file {1}",
                new Object[] { name, datafile });
        try {
            Set<Car> cars = loadData(name, datafile);
            
            //use persistence
            Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
            Key crcKey = datastore.newKeyFactory().setKind("CarRentalCompany").newKey(name);
            
            for(Car car : cars) {
            	// set cars
            	Key carKey = datastore.newKeyFactory()
            			.addAncestor(PathElement.of("CarRentalCompany", name))
            			.setKind("Car")
            			.newKey(car.getId());
            	Entity carEntity = Entity.newBuilder(carKey)
            			.set("carType", car.getType().getName())
            			.build();
            	datastore.put(carEntity);
            	
            	// set carTypes
            	Key carTypeKey = datastore.newKeyFactory()
            			.addAncestor(PathElement.of("CarRentalCompany", name))
            			.setKind("CarType")
            			.newKey(car.getType().getName());
            	if(datastore.get(carTypeKey) == null) {
            		Entity carTypeEntity = Entity.newBuilder(carTypeKey)
            				.set("price", car.getType().getRentalPricePerDay())
            				.set("space", car.getType().getTrunkSpace())
            				.set("smokeornot", car.getType().isSmokingAllowed())
            				.set("nbofseat", car.getType().getNbOfSeats())
            				.build();
            		datastore.put(carTypeEntity);
            	}
            }
            Entity crc = Entity.newBuilder(crcKey)
            		.set("name", name)
            		.build();
            datastore.put(crc);
            CarRentalModel.get().CRCS.add(crc);           
            
        } catch (NumberFormatException ex) {
            Logger.getLogger(CarRentalServletContextListener.class.getName()).log(Level.SEVERE, "bad file", ex);
        } catch (IOException ex) {
            Logger.getLogger(CarRentalServletContextListener.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

	public static Set<Car> loadData(String name, String datafile) throws NumberFormatException, IOException {
        Set<Car> cars = new HashSet<Car>();
        int carId = 1;

        // open file from jar
        BufferedReader in = new BufferedReader(new InputStreamReader(
                CarRentalServletContextListener.class.getClassLoader().getResourceAsStream(datafile)));
        // while next line exists
        while (in.ready()) {
            // read line
            String line = in.readLine();
            // if comment: skip
            if (line.startsWith("#")) {
                continue;
            }
            // tokenize on ,
            StringTokenizer csvReader = new StringTokenizer(line, ",");
            // create new car type from first 5 fields
            CarType type = new CarType(csvReader.nextToken(), Integer.parseInt(csvReader.nextToken()),
                    Float.parseFloat(csvReader.nextToken()), Double.parseDouble(csvReader.nextToken()),
                    Boolean.parseBoolean(csvReader.nextToken()));
            // create N new cars with given type, where N is the 5th field
            for (int i = Integer.parseInt(csvReader.nextToken()); i > 0; i--) {
                cars.add(new Car(carId++, type));
            }
        }

        return cars;
    }

    @Override
    public void contextDestroyed(ServletContextEvent arg0) {
        // Please leave this method empty.
    }
}
