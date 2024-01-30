from concurrent import futures
from datetime import datetime
from threading import Semaphore

import redis
import grpc
import inventory_pb2
import inventory_pb2_grpc


# Global lock (allows one thread access)
LOCK = Semaphore(1)

class InventoryServiceServicer(inventory_pb2_grpc.InventoryServiceServicer):
    """
    Implements the methods to handle inventory management operations.

    Methods:
        AddProduct: Adds a product to the inventory.
        GetProductById: Retrieves a product from the inventory by ID.
        UpdateProductQuantity: Updates the quantity of a product in the inventory.
        DeleteProduct: Deletes a product from the inventory.
        GetAllProducts: Retrieves all products from the inventory.
    """
    
    def AddProduct(self, request, context):
        """
        Adds a product to the inventory.

        Args:
            request (inventory_pb2.Product): The product to add.
            context (grpc.ServicerContext): Context of the gRPC call.

        Returns:
            inventory_pb2.Status: Status of the operation.
        """
        if request.product_identifier < 0:
            return inventory_pb2.Status(status="Cannot have a Product ID less than 0.")
        try:
            print(f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} Request: Add Product\n', fr"{{'product_identifier : '{request.product_identifier}', 'product_name' : '{request.product_name}', 'product_quantity' : '{request.product_quantity}', 'product_price' : '{request.product_price: .2f}'}}")
            r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        
            # Return Status if product exists
            if r.exists(request.product_identifier) > 0:
                print(f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} Product exists, NOT added.')
                return inventory_pb2.Status(status="Product already exists and was NOT added. \nTry deleting the product first, or change the Product ID.")
            
            # Add product and return status 
            result = r.hset(request.product_identifier, mapping={
                    'product_name': request.product_name,
                    'product_quantity': request.product_quantity,
                    'product_price': request.product_price
                    })
            
            print(f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} Product added.')
            return inventory_pb2.Status(status="Product successfully added.")
            
        except ConnectionError:
            print(f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} Error connecting to redis-server. Maybe the server wasn\'t started?')
            return inventory_pb2.Status(status="Server failure. Product was NOT added.")
        
        
    def GetProductById(self, request, context):
        """
        Retrieves a product from the inventory by ID.

        Args:
            request (inventory_pb2.Product): The product request containing the ID.
            context (grpc.ServicerContext): Context of the gRPC call.

        Returns:
            inventory_pb2.Product: The retrieved product.
        """
        try:
            print(f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} Request: Product ID {request.product_identifier}')
            r = redis.Redis(host='localhost', port=6379, decode_responses=True)

            LOCK.acquire()
            # Return empty NULL product if product_identifier does not exist
            if r.exists(request.product_identifier) < 1:
                LOCK.release()
                print(f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} Product does not exist.')
                return inventory_pb2.Product(product_identifier=-1, product_name="NULL", 
                                            product_quantity=-1, product_price=float(-1))
            
            # Locate and return product
            result = r.hgetall(request.product_identifier)
            LOCK.release()
            
            print(f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} Product found.')
            return inventory_pb2.Product(product_identifier=int(request.product_identifier), product_name=str(result.get('product_name')), 
                                        product_quantity=int(result.get('product_quantity')), product_price=float(result.get('product_price')))
           
        except ConnectionError:
            print(f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} Error connecting to redis-server. Maybe the server wasn\'t started?')
            return inventory_pb2.Product(product_identifier=-2, product_name="NULL", 
                                            product_quantity=-1, product_price=float(-1))
            
        
    def UpdateProductQuantity(self, request, context):
        """
        Updates the quantity of a product in the inventory.

        Args:
            request (inventory_pb2.Product): The product request containing the ID and new quantity.
            context (grpc.ServicerContext): Context of the gRPC call.

        Returns:
            inventory_pb2.Product: The updated product.
        """
        try:
            print(f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} Request: Update Product Quantity \n\tProduct ID: {request.product_identifier} \n\tQuantity: {request.product_quantity}')
            r = redis.Redis(host='localhost', port=6379, decode_responses=True)
            
            LOCK.acquire()
            # Return empty NULL product if product_identifier does not exist
            if r.exists(request.product_identifier) < 1:
                LOCK.release()
                print(f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} Product does not exist.')
                return inventory_pb2.Product(product_identifier=-1, product_name="NULL", 
                                            product_quantity=-1, product_price=float(-1))
                
            # Locate, update quantity, and return product
            result = r.hset(request.product_identifier, 'product_quantity', request.product_quantity)
            LOCK.release()
            
            print(f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} Product found, quantity updated.')
            return inventory_pb2.Product(product_identifier=int(request.product_identifier), product_name=str(result.get('product_name')), 
                                        product_quantity=int(result.get('product_quantity')), product_price=float(result.get('product_price')))
            
        except ConnectionError:
            print(f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} Error connecting to redis-server. Maybe the server wasn\'t started?')
            return inventory_pb2.Product(product_identifier=-2, product_name="NULL", 
                                            product_quantity=-1, product_price=float(-1))
    
    
    def DeleteProduct(self, request, context):
        """
        Deletes a product from the inventory.

        Args:
            request (inventory_pb2.Product): The product request containing the ID.
            context (grpc.ServicerContext): Context of the gRPC call.

        Returns:
            inventory_pb2.Status: Status of the operation.
        """
        try:
            print(f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} Request: Delete Product ID {request.product_identifier}')
            r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        
            LOCK.acquire()
            # Return Status if product does not exists
            if r.exists(request.product_identifier) < 1:
                LOCK.release()
                print(f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} Product does not exist, NOT deleted.')
                return inventory_pb2.Status(status="Product does not exist and was NOT deleted.")
            
            # Delete product and return status 
            result = r.delete(request.product_identifier)
            LOCK.release()
            
            print(f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} Product deleted.')
            return inventory_pb2.Status(status="Product successfully deleted.")
        
        except ConnectionError:
            print(f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} Error connecting to redis-server. Maybe the server wasn\'t started?')
            return inventory_pb2.Status(status="Server failure. Product was NOT deleted.")
        
    
    def GetAllProducts(self, request, context):
        """
        Retrieves all products from the inventory.

        Args:
            request: Empty message.
            context (grpc.ServicerContext): Context of the gRPC call.

        Yields:
            inventory_pb2.Product: The products in the inventory.
        
        Returns:
            inventory_pb2.Product: A NULL generated product.
        """
        try:
            print(f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} Request: All Products')
            r = redis.Redis(host='localhost', port=6379, decode_responses=True)
            LOCK.acquire()
            products = r.keys('*') # Return list of keys that match wildcard (return all keys)
            
            # Return empty NULL product if database is empty
            if len(products) <= 0:
                LOCK.release()
                print(f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} Redis server contains no data at the moment.')
                yield inventory_pb2.Product(product_identifier=-1, product_name="NULL", 
                                                product_quantity=-1, product_price=float(-1))
                return None
           
            # Stream out products in database
            print(f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} Streaming out all products.')
            for i in range(len(products)):
                result = r.hgetall(products[i])
                yield inventory_pb2.Product(product_identifier=int(products[i]), product_name=str(result.get('product_name')), 
                                            product_quantity=int(result.get('product_quantity')), product_price=float(result.get('product_price')))
            LOCK.release()
            
        except ConnectionError:
            print(f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} Error connecting to redis-server. Maybe the server wasn\'t started?')
            return inventory_pb2.Product(product_identifier=-2, product_name="NULL", 
                                            product_quantity=-1, product_price=float(-1))
        except(e):
            print(f"All other except: {e}")
    


def serve():
    """
    Initializes and starts the gRPC server.

    Returns:
        grpc.Server: The initialized gRPC server.
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    inventory_pb2_grpc.add_InventoryServiceServicer_to_server(InventoryServiceServicer(), server)
    server.add_insecure_port('[::]:50051')
    return server


if __name__ == '__main__':
    try:
        server = serve()
        server.start()
        print(f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} Server Started...')
        print(f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")} Kill with keyboard interrupt (Ctrl+C)')
        server.wait_for_termination()
        
    except KeyboardInterrupt:
        server.stop(0)
        exit(0)
    except:
        print(f'{datetime.now().strftime("%d/%m/%Y %H:%M:%S")}An error occurred initiating the server')
        exit(0)