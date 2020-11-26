package ds.gae;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.PathElement;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.StructuredQuery.PropertyFilter;

import ds.gae.entities.Car;
import ds.gae.entities.CarRentalCompany;
import ds.gae.entities.CarType;
import ds.gae.entities.Quote;
import ds.gae.entities.Reservation;
import ds.gae.entities.ReservationConstraints;

public class CarRentalModel {

    //use persistence
	public List<Entity> CRCS = new ArrayList<>();

    private static CarRentalModel instance;

    public static CarRentalModel get() {
        if (instance == null) {
            instance = new CarRentalModel();
        }
        return instance;
    }

    /**
     * Get the car types available in the given car rental company.
     *
     * @param companyName the car rental company
     * @return The list of car types (i.e. name of car type), available in the given
     * car rental company.
     */
    public Set<String> getCarTypesNames(String companyName) {
    	
        Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
        Key key = null;
        Set<String> carTypes = new HashSet<>();
        
        for(Entity crc : CRCS) {
        	if(crc.getKey().getName() == companyName) {
        		key = crc.getKey();
        		break;
        	}
        }
        
        Query<Entity> query = Query.newEntityQueryBuilder()
        		.setKind("CarType")
        		.setFilter(PropertyFilter.hasAncestor(key))
        		.build();
        QueryResults<Entity> results = datastore.run(query);
        
        while(results.hasNext()) {
        	Entity e = results.next();
        	carTypes.add(e.getKey().getName());
        }
        
        return carTypes;
        
    }

    /**
     * Get the names of all registered car rental companies
     *
     * @return the list of car rental companies
     */
    public Collection<String> getAllRentalCompanyNames() {
        Collection<String> crcs = Collections.emptyList();
        
        for(Entity crc : CRCS) {
        	crcs.add(crc.getKey().getName());
        }
        
        return crcs;
    }

    /**
     * Create a quote according to the given reservation constraints (tentative
     * reservation).
     *
     * @param companyName name of the car renter company
     * @param renterName  name of the car renter
     * @param constraints reservation constraints for the quote
     * @return The newly created quote.
     * @throws ReservationException No car available that fits the given
     *                              constraints.
     */
    public Quote createQuote(String companyName, String renterName, ReservationConstraints constraints)
            throws ReservationException {
        //use persistence
    	Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
        
        KeyFactory reservationKeyF = datastore.newKeyFactory()
    			.addAncestor(PathElement.of("CarRentalCompany", companyName))
    			.setKind("Reservation");
        Key reservationKey = datastore.allocateId(reservationKeyF.newKey());
        Entity reservationEntity = Entity.newBuilder(reservationKey)
				.set("crc", companyName)
        		.set("client", renterName)
        		.set("end", constraints.getEndDate().toString())
        		.set("start", constraints.getStartDate().toString())
        		.set("cartype",constraints.getCarType())
				.build();
		datastore.put(reservationEntity);
		
    	Key carTypeKey = datastore.newKeyFactory()
    			.addAncestor(PathElement.of("CarRentalCompany", companyName))
    			.setKind("CarType")
    			.newKey(constraints.getCarType());
    	Entity ce = datastore.get(carTypeKey);
    	double price = (double)ce.getDouble("price");
		
		Quote quote = new Quote(renterName,constraints.getStartDate(),constraints.getEndDate(),companyName,constraints.getCarType(),price);
		return quote;
    }

    /**
     * Confirm the given quote.
     *
     * @param quote Quote to confirm
     * @throws ReservationException Confirmation of given quote failed.
     */
    public void confirmQuote(Quote quote) throws ReservationException {
        // FIXME: use persistence instead
        //CarRentalCompany crc = CRCS.get(quote.getRentalCompany());
        //crc.confirmQuote(quote);
    }

    /**
     * Confirm the given list of quotes
     *
     * @param quotes the quotes to confirm
     * @return The list of reservations, resulting from confirming all given quotes.
     * @throws ReservationException One of the quotes cannot be confirmed. Therefore
     *                              none of the given quotes is confirmed.
     */
    public List<Reservation> confirmQuotes(List<Quote> quotes) throws ReservationException {
        // TODO: add implementation when time left, required for GAE2
        return null;
    }

    /**
     * Get all reservations made by the given car renter.
     *
     * @param renter name of the car renter
     * @return the list of reservations of the given car renter
     * @throws Exception 
     */
    public List<Reservation> getReservations(String renter) throws Exception {
    	
    	List<Reservation> rlist = new ArrayList<>();
    	
    	Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
        
        Query<Entity> query = Query.newEntityQueryBuilder()
        		.setKind("CarType")
        		.setFilter(PropertyFilter.eq("client", renter))
        		.build();
        QueryResults<Entity> results = datastore.run(query);
        
        while(results.hasNext()) {
        	Entity e = results.next();
        	Date start = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy").parse(e.getString("start"));
        	Date end = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy").parse(e.getString("end"));
        	String crc = e.getString("crc");
        	String carType = e.getString("cartype");
        	
        	Key carTypeKey = datastore.newKeyFactory()
        			.addAncestor(PathElement.of("CarRentalCompany", crc))
        			.setKind("CarType")
        			.newKey(carType);
        	Entity ce = datastore.get(carTypeKey);
        	double price = (double)ce.getDouble("price");
        	
        	Reservation cte = new Reservation(renter,start,end,crc,carType,price);
        	rlist.add(cte);
        	
        }
        
        return rlist;
    }

    /**
     * Get the car types available in the given car rental company.
     *
     * @param companyName the given car rental company
     * @return The list of car types in the given car rental company.
     */
    public Collection<CarType> getCarTypesOfCarRentalCompany(String companyName) {
    	Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
        
    	Collection<CarType> ct = new ArrayList<>();
    	
    	Key key = null;
    	Set<String> carTypes = new HashSet<>();

    	for(Entity crc : CRCS) {
    		if(crc.getKey().getName() == companyName) {
    			key = crc.getKey();
    			break;
    		}
    	}

        Query<Entity> query = Query.newEntityQueryBuilder()
        		.setKind("CarType")
        		.setFilter(PropertyFilter.hasAncestor(key))
        		.build();
        QueryResults<Entity> results = datastore.run(query);
        
        while(results.hasNext()) {
        	Entity e = results.next();
        	Boolean smoke = e.getBoolean("smokeornot");
        	Double price = e.getDouble("price");
        	float space = e.getLong("space");
        	int nbofseat = (int) e.getDouble("nbofseat");
        	String name = e.getKey().getName();
        	
        	CarType cte = new CarType(name,nbofseat,space,price,smoke);
        	ct.add(cte);
        	
        }
        
        return ct;
    }

    /**
     * Get the list of cars of the given car type in the given car rental company.
     *
     * @param companyName name of the car rental company
     * @param carType     the given car type
     * @return A list of car IDs of cars with the given car type.
     */
    public Collection<Integer> getCarIdsByCarType(String companyName, CarType carType) {
        Collection<Integer> out = new ArrayList<>();
        for (Car c : getCarsByCarType(companyName, carType)) {
            out.add(c.getId());
        }
        return out;
    }

    /**
     * Get the amount of cars of the given car type in the given car rental company.
     *
     * @param companyName name of the car rental company
     * @param carType     the given car type
     * @return A number, representing the amount of cars of the given car type.
     */
    public int getAmountOfCarsByCarType(String companyName, CarType carType) {
        return this.getCarsByCarType(companyName, carType).size();
    }

    /**
     * Get the list of cars of the given car type in the given car rental company.
     *
     * @param companyName name of the car rental company
     * @param carType     the given car type
     * @return List of cars of the given car type
     */
    private List<Car> getCarsByCarType(String companyName, CarType carType) {
        
    	Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
        
    	List<Car> cars = new ArrayList<>();
    	
    	Key key = null;

    	for(Entity crc : CRCS) {
    		if(crc.getKey().getName() == companyName) {
    			key = crc.getKey();
    			break;
    		}
    	}

        Query<Entity> query = Query.newEntityQueryBuilder()
        		.setKind("Car")
        		.setFilter(PropertyFilter.hasAncestor(key))
        		.setFilter(PropertyFilter.eq("carType", carType.getName()))
        		.build();
        QueryResults<Entity> results = datastore.run(query);
        
        while(results.hasNext()) {
        	Entity e = results.next();
        	int uid = e.getKey().getId().intValue();
        	Car car = new Car(uid,carType);
        	cars.add(car);
        }
        
        return cars;

    }

    /**
     * Check whether the given car renter has reservations.
     *
     * @param renter the car renter
     * @return True if the number of reservations of the given car renter is higher
     * than 0. False otherwise.
     * @throws Exception 
     */
    public boolean hasReservations(String renter) throws Exception {
        return this.getReservations(renter).size() > 0;
    }
}
