import grpc
from concurrent import futures
import redis
import inventory_pb2
import inventory_pb2_grpc

class InventoryServiceServicer(inventory_pb2_grpc.InventoryServiceServicer):
    def AddProduct(self, request, context):
        print("Request: Add Product\n", fr"{{'product_identifier : '{request.product_identifier}', 'product_name' : '{request.product_name}', 'product_quantity' : '{request.product_quantity}', 'product_price' : '{request.product_price: .2f}'}}")
        r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        result = r.hset(request.product_identifier, mapping={
                 'product_name': request.product_name,
                 'product_quantity': request.product_quantity,
                 'product_price': request.product_price
                 })
        return inventory_pb2.Status(status=True)
        
        
    def GetProductById(self, request, context):
        print(f"Request: Product ID {request.product_identifier}")
        r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        result = r.hgetall(request.product_identifier)
        print(f"Result: {result}")
        return inventory_pb2.Product(product_identifier=int(request.product_identifier), product_name=str(result.get('product_name')), 
                                     product_quantity=int(result.get('product_quantity')), product_price=float(result.get('product_price')))
        
        
    def UpdateProductQuantity(self, request, context):
        print(f"Request: Update Product Quantity \n\tProduct ID: {request.product_identifier} \n\tQuantity: {request.product_quantity}")
        r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        result = r.hset(request.product_identifier, 'product_quantity', request.product_quantity)
        print(f"Result: {result}")
    
    
    def DeleteProduct(self, request, context):
        print(f"Request: Delete Product ID {request.product_identifier}")
        pass
    
    def GetAllProducts(self, request, context):
        print(f"Request: All Products")
        r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        products = r.keys('*') # Return list of keys that match wildcard (return all keys)
        
        for i in range(len(products)):
            result = r.hgetall(products[i])
            yield inventory_pb2.Product(product_identifier=int(products[i]), product_name=str(result.get('product_name')), 
                                        product_quantity=int(result.get('product_quantity')), product_price=float(result.get('product_price')))
        


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    inventory_pb2_grpc.add_InventoryServiceServicer_to_server(InventoryServiceServicer(), server)
    server.add_insecure_port('[::]:50051')
    return server


if __name__ == '__main__':
    try:
        server = serve()
        server.start()
        print('Server Started...')
        print('Kill with keyboard interrupt (Ctrl+C)')
        server.wait_for_termination()
        
    except KeyboardInterrupt:
        server.stop(0)
        exit(0)
    except:
        print('An error occurred initiating the server')
        exit(0)