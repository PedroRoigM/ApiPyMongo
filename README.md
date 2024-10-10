### LocalStore

Welcome to the LocalStore repository! This project is dedicated to developing an integrated management platform for an online sales website. Our team, 'DATA,' is responsible for designing and implementing the database system to manage platform information, as well as the design and implementation of models and mapping interfaces for database communication.

### Requirements

The client requires the platform to manage information for products, customers, purchases, and suppliers, with the following attributes:

- *Customer*: Name, billing addresses, shipping addresses, payment cards, registration date, last access date.
- *Product*: Name, supplier product code, price (with and without VAT), shipping cost, discount by date range, dimensions, weight, suppliers/warehouses, and other specific attributes.
- *Purchase*: Products, customer, purchase price, purchase date, shipping address.
- *Supplier*: Name, warehouse addresses.

As attributes may increase over time, the database should be flexible enough to accommodate new attributes. Some attributes might not be common to all contacts and may not have values for some.

### Features

The ODM should include:

- *Model Class*:
  - save(): Stores data in the database, inserting new documents and updating modified fields in existing ones. Coordinates are stored with addresses for geolocation queries.
  - delete(): Deletes a document from the database.
  - find(): Class method that queries the database and returns a ModelCursor object for results in model format.
  - __setattr__(): Modifies model data in memory, controlling attributes so that only new information is sent to the database.

- *ModelCursor Class*:
  - __iter__(): Returns an iterator that goes through elements from the pymongo cursor, returning documents as model objects using yield and next.
  - Uses the alive attribute of CommandCursor to check for more documents.

- *initApp Function*: Declares and initializes classes inheriting from the abstract Model class for necessary collections (person, company, educational center).
- *getLocationPoint Function*: Obtains coordinates for an address in geojson.Point format using the geopy API, considering limited access and avoiding mass queries.

### Considerations

- Locations should be implemented following the geoJSON format and indexed using 2dsphere.

### Templates

Two template files are provided for this practice:
- A YAML file with model definitions and their accepted and required variables.
- A Python file template for the Model and ModelCursor classes and the initApp and getLocationPoint functions to be developed by the student.

---

### Getting Started

To get started, clone this repository and follow the instructions in the provided templates to develop the required functionality.
