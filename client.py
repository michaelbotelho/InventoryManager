from os import system
#from pick.src.pick import pick # https://github.com/wong2/pick
from pick import pick
import grpc
import inventory_pb2
import inventory_pb2_grpc
import google.protobuf.empty_pb2


def run(opcode):
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = inventory_pb2_grpc.InventoryServiceStub(channel)
        try:
            if opcode == 0: # AddProduct Request
                pid, pname, pquant, pprice = prompt(['id','name','quantity','price'])
                response = stub.AddProduct(inventory_pb2.Product(product_identifier=pid, product_name=pname, product_quantity=pquant, product_price=pprice))
                title = f"Recieved: Product successfully added {response.status}"
                
            elif opcode == 1: # GetProductById Request
                pid = prompt(['id'])
                response = stub.GetProductById(inventory_pb2.ProductIdentifier(product_identifier=pid))
                title = f"Recieved:\n\tProduct ID: {response.product_identifier} \n\tProduct Name: {response.product_name}\n\tProduct Quantity: x{response.product_quantity}\n\tProduct Price: ${response.product_price:.2f}"
            
            elif opcode == 2: # UpdateProductQuantity Request
                pid, pquant = prompt(['id', 'quantity'])
                response = stub.UpdateProductQuantity(inventory_pb2.Quantity(product_identifier=pid, product_quantity=pquant))
                title = f"Recieved: Product Quantity successfully updated {response.status}"
            
            elif opcode == 3: # DeleteProduct Request
                pid = prompt(['id'])
                response = stub.DeleteProduct(inventory_pb2.ProductIdentifier(product_identifier=pid))
                pass
            
            elif opcode == 4: # GetAllProducts Request
                response_iterator = stub.GetAllProducts(google.protobuf.empty_pb2.Empty())
                for response in response_iterator:
                    print(f"\nRecieved:\n\tProduct ID: {response.product_identifier} \n\tProduct Name: {response.product_name}\n\tProduct Quantity: x{response.product_quantity}\n\tProduct Price: ${response.product_price:.2f}")

                if input("Continue using program? [Y/n]").lower() != 'y':
                    pass
                else:
                    exit(0)
                
            elif opcode == 5: # Exit program code
                exit(0)
              
                
            # Generating selection menu          
            title += '\n\n What would you like to do '
            options = ['Add Product', 'Find Product By ID', 'Update Product Quantity', 'Delete Product', 'Find All Products', 'Quit']
            option, opcode = pick(options, title, indicator='=>')       
            
            run(opcode) # Looping
            
        except TypeError:
            if input("One or more inputs are of incorrect type. Continue? [Y/n]").lower() != 'y':
                system('cls')
                exit(0)
            else:
                run(prompt())
        except KeyboardInterrupt:
            exit(0)
                

        

# Prompt user for types of information (including functionality selection)
# Support for id, name, quantity, and price
def prompt(type=0):
    try:
        # Generating Selection menu
        if type == 0:
            title = 'What would you like to do '
            options = ['Add Product', 'Find Product By ID', 'Update Product Quantity', 'Delete Product', 'Find All Products', 'Quit']
            option, opcode = pick(options, title, indicator='=>')

            return opcode
        
        # Prompting user for information given by type    
        else:
            system('cls')
            res = []
            for each in type:
                if each == 'id':
                    res.append(int(input('Please input the Product ID: ')))
                elif each == 'name':
                    res.append(str(input('Please input the Product Name: ')))
                elif each == 'quantity':
                    res.append(int(input('Please input the Product Quantity: ')))
                elif each == 'price':
                    res.append(float(input('Please input the Product Price: ')))
                    
            if len(res) == 1: # Remove item from res when returning one object for cleaner use
                return res[0]
            return res
    
    except KeyboardInterrupt:
        exit(0)
    except:
        if input("One or more inputs are of incorrect type. Continue? [Y/n]").lower() == 'y':
            prompt()
            system('cls')
        else:
            system('cls')
            exit(0)
            
    
    
if __name__ == '__main__':
    try:
        run(prompt()) # First call to program loop
    except:
        exit(0)