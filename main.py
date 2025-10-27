# main.py
from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr, Field
from typing import Optional, List, Literal
from datetime import datetime, timedelta
from enum import Enum
import uuid
from passlib.context import CryptContext
import random

# ============================================================================
# CONFIGURATION
# ============================================================================

app = FastAPI(title="Hedging Platform API", version="1.0.0")

# CORS Configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# ============================================================================
# ENUMS
# ============================================================================

class UserType(str, Enum):
    FARMER = "farmer"
    TRADER = "trader"

class ProductType(str, Enum):
    SOYBEAN = "Soybean"
    SUNFLOWER = "Sunflower"
    GROUNDNUT = "Groundnut"
    MUSTARD = "Mustard"
    SESAME = "Sesame"

class UnitType(str, Enum):
    KG = "kg"
    TONNE = "tonne"

class ContractStatus(str, Enum):
    PENDING = "PENDING"
    ACTIVE = "ACTIVE"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"
    COMPLETED = "COMPLETED"

class CreatedBy(str, Enum):
    FARMER = "farmer"
    TRADER = "trader"

# ============================================================================
# PYDANTIC MODELS (Request/Response)
# ============================================================================

# Auth Models
class RegisterRequest(BaseModel):
    email: EmailStr
    type: UserType
    password: str
    name: str
    phone: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    pincode: Optional[str] = None

class LoginRequest(BaseModel):
    email: EmailStr
    password: str

class UserResponse(BaseModel):
    _id: str
    email: str
    type: str
    name: str
    phone: Optional[str]
    location: Optional[dict]
    created_at: str

# Product Models
class ListProductRequest(BaseModel):
    type: ProductType
    qty: float = Field(gt=0)
    unit: UnitType

class ProductResponse(BaseModel):
    _id: str
    farmer_id: str
    type: str
    total_qty: float
    available_qty: float
    reserved_qty: float
    committed_qty: float
    unit: str
    is_active: bool
    created_at: str
    updated_at: str

# Contract Models
class CreateContractByFarmerRequest(BaseModel):
    product_id: str
    price_per_unit: float = Field(gt=0)
    qty: float = Field(gt=0)
    unit: UnitType
    expected_delivery_date: Optional[str] = None
    notes: Optional[str] = None

class CreateContractByTraderRequest(BaseModel):
    farmer_id: str
    product_id: str
    price_per_unit: float = Field(gt=0)
    qty: float = Field(gt=0)
    unit: UnitType
    expected_delivery_date: Optional[str] = None
    notes: Optional[str] = None

class AcceptContractRequest(BaseModel):
    contract_id: str

class RejectContractRequest(BaseModel):
    contract_id: str
    rejection_reason: Optional[str] = None

class CancelContractRequest(BaseModel):
    contract_id: str

class CompleteContractRequest(BaseModel):
    contract_id: str
    completed_by: CreatedBy

class ContractResponse(BaseModel):
    _id: str
    farmer_id: str
    trader_id: Optional[str]
    product_id: str
    product_type: str
    price_per_unit: float
    qty: float
    unit: str
    total_value: float
    status: str
    created_by: str
    created_at: str
    accepted_at: Optional[str] = None
    accepted_by: Optional[str] = None
    completed_at: Optional[str] = None
    rejected_at: Optional[str] = None
    cancelled_at: Optional[str] = None

# ============================================================================
# IN-MEMORY DATABASE
# ============================================================================

# Collections
users_db = {}
products_db = {}
contracts_db = {}
market_prices_db = []
forecasts_db = []

# Session storage (email -> user_id mapping for simple auth)
sessions = {}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def hash_password(password: str) -> str:
    if len(password) > 72:
        password = password[:72]
    return pwd_context.hash(password)

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)

def get_current_user_email(email: str) -> dict:
    """Simple auth - get user by email from session"""
    if email not in sessions:
        raise HTTPException(status_code=401, detail="Not authenticated")
    user_id = sessions[email]
    if user_id not in users_db:
        raise HTTPException(status_code=401, detail="User not found")
    return users_db[user_id]

def generate_id() -> str:
    return str(uuid.uuid4())

# ============================================================================
# SEED DATA FUNCTION
# ============================================================================

def seed_data():
    """Seed initial data for testing"""
    
    # Create sample users
    farmer1_id = generate_id()
    farmer2_id = generate_id()
    trader1_id = generate_id()
    trader2_id = generate_id()
    
    users_db[farmer1_id] = {
        "_id": farmer1_id,
        "email": "farmer1@test.com",
        "password": hash_password("password123"),
        "type": "farmer",
        "name": "Rajesh Kumar",
        "phone": "+919876543210",
        "location": {"city": "Indore", "state": "Madhya Pradesh", "pincode": "452001"},
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat()
    }
    
    users_db[farmer2_id] = {
        "_id": farmer2_id,
        "email": "farmer2@test.com",
        "password": hash_password("password123"),
        "type": "farmer",
        "name": "Suresh Patel",
        "phone": "+919876543211",
        "location": {"city": "Bhopal", "state": "Madhya Pradesh", "pincode": "462001"},
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat()
    }
    
    users_db[trader1_id] = {
        "_id": trader1_id,
        "email": "trader1@test.com",
        "password": hash_password("password123"),
        "type": "trader",
        "name": "Arun Traders Pvt Ltd",
        "phone": "+919876543212",
        "location": {"city": "Mumbai", "state": "Maharashtra", "pincode": "400001"},
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat()
    }
    
    users_db[trader2_id] = {
        "_id": trader2_id,
        "email": "trader2@test.com",
        "password": hash_password("password123"),
        "type": "trader",
        "name": "Vikram Trading Co",
        "phone": "+919876543213",
        "location": {"city": "Delhi", "state": "Delhi", "pincode": "110001"},
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat()
    }
    
    # Create sample products
    product1_id = generate_id()
    product2_id = generate_id()
    product3_id = generate_id()
    
    products_db[product1_id] = {
        "_id": product1_id,
        "farmer_id": farmer1_id,
        "type": "Soybean",
        "total_qty": 1000,
        "available_qty": 1000,
        "reserved_qty": 0,
        "committed_qty": 0,
        "unit": "kg",
        "is_active": True,
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat()
    }
    
    products_db[product2_id] = {
        "_id": product2_id,
        "farmer_id": farmer1_id,
        "type": "Mustard",
        "total_qty": 500,
        "available_qty": 500,
        "reserved_qty": 0,
        "committed_qty": 0,
        "unit": "kg",
        "is_active": True,
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat()
    }
    
    products_db[product3_id] = {
        "_id": product3_id,
        "farmer_id": farmer2_id,
        "type": "Groundnut",
        "total_qty": 800,
        "available_qty": 800,
        "reserved_qty": 0,
        "committed_qty": 0,
        "unit": "kg",
        "is_active": True,
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat()
    }
    
    # Generate market prices for last 90 days
    product_types = ["Soybean", "Sunflower", "Groundnut", "Mustard", "Sesame"]
    base_prices = {
        "Soybean": 45.0,
        "Sunflower": 52.0,
        "Groundnut": 60.0,
        "Mustard": 55.0,
        "Sesame": 70.0
    }
    
    for product_type in product_types:
        base_price = base_prices[product_type]
        for i in range(90, 0, -1):
            date = datetime.now() - timedelta(days=i)
            # Add some randomness to prices
            price = base_price + random.uniform(-3, 3) + (random.random() - 0.5) * 2
            market_prices_db.append({
                "_id": generate_id(),
                "product_type": product_type,
                "price": round(price, 2),
                "unit": "kg",
                "date": date.date().isoformat(),
                "source": "Agmarknet",
                "created_at": date.isoformat()
            })
    
    # Generate forecasts for next 45 days
    for product_type in product_types:
        base_price = base_prices[product_type]
        # Current price (last price from history)
        current_prices = [p for p in market_prices_db if p["product_type"] == product_type]
        if current_prices:
            current_price = current_prices[-1]["price"]
        else:
            current_price = base_price
        
        for i in range(1, 46):
            forecast_date = datetime.now() + timedelta(days=i)
            # Simulate trend with some randomness
            trend = i * 0.05  # Slight upward trend
            predicted_price = current_price + trend + random.uniform(-1, 1)
            
            forecasts_db.append({
                "_id": generate_id(),
                "product_type": product_type,
                "forecast_date": forecast_date.date().isoformat(),
                "predicted_price": round(predicted_price, 2),
                "confidence_lower": round(predicted_price - random.uniform(2, 4), 2),
                "confidence_upper": round(predicted_price + random.uniform(2, 4), 2),
                "model_version": "prophet_v1",
                "unit": "kg",
                "generated_at": datetime.now().isoformat()
            })

# Seed data on startup
seed_data()

# ============================================================================
# AUTH ENDPOINTS
# ============================================================================

@app.post("/api/auth/register", response_model=dict)
async def register(request: RegisterRequest):
    """Register a new user"""
    
    # Check if email already exists
    for user in users_db.values():
        if user["email"] == request.email:
            raise HTTPException(status_code=400, detail="Email already registered")
    
    # Create new user
    user_id = generate_id()
    location = None
    if request.city or request.state or request.pincode:
        location = {
            "city": request.city,
            "state": request.state,
            "pincode": request.pincode
        }
    
    user = {
        "_id": user_id,
        "email": request.email,
        "password": hash_password(request.password),
        "type": request.type.value,
        "name": request.name,
        "phone": request.phone,
        "location": location,
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat()
    }
    
    users_db[user_id] = user
    sessions[request.email] = user_id
    
    return {
        "success": True,
        "message": "User registered successfully",
        "user": {
            "_id": user["_id"],
            "email": user["email"],
            "type": user["type"],
            "name": user["name"]
        }
    }

@app.post("/api/auth/login", response_model=dict)
async def login(request: LoginRequest):
    """Login user"""
    
    # Find user by email
    user = None
    for u in users_db.values():
        if u["email"] == request.email:
            user = u
            break
    
    if not user or not verify_password(request.password, user["password"]):
        raise HTTPException(status_code=401, detail="Invalid email or password")
    
    # Create session
    sessions[request.email] = user["_id"]
    
    return {
        "success": True,
        "user": {
            "_id": user["_id"],
            "email": user["email"],
            "type": user["type"],
            "name": user["name"],
            "phone": user.get("phone"),
            "location": user.get("location")
        }
    }

# ============================================================================
# PRODUCT ENDPOINTS
# ============================================================================

@app.post("/api/farmer/list-product", response_model=dict)
async def list_product(request: ListProductRequest, email: str):
    """Farmer lists a new product"""
    
    user = get_current_user_email(email)
    
    if user["type"] != "farmer":
        raise HTTPException(status_code=403, detail="Only farmers can list products")
    
    product_id = generate_id()
    product = {
        "_id": product_id,
        "farmer_id": user["_id"],
        "type": request.type.value,
        "total_qty": request.qty,
        "available_qty": request.qty,
        "reserved_qty": 0,
        "committed_qty": 0,
        "unit": request.unit.value,
        "is_active": True,
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat()
    }
    
    products_db[product_id] = product
    
    return {
        "success": True,
        "message": "Product listed successfully",
        "product": product
    }

@app.get("/api/farmer/products", response_model=dict)
async def get_farmer_products(email: str, farmer_id: Optional[str] = None):
    """Get all products for a farmer"""
    
    user = get_current_user_email(email)
    
    # If farmer_id is provided, use it; otherwise use current user's id
    target_farmer_id = farmer_id if farmer_id else user["_id"]
    
    products = [p for p in products_db.values() if p["farmer_id"] == target_farmer_id]
    
    # Add contract counts
    for product in products:
        product["active_contracts_count"] = len([
            c for c in contracts_db.values() 
            if c["product_id"] == product["_id"] and c["status"] == "ACTIVE"
        ])
        product["pending_contracts_count"] = len([
            c for c in contracts_db.values() 
            if c["product_id"] == product["_id"] and c["status"] == "PENDING"
        ])
    
    return {
        "success": True,
        "products": products
    }

@app.get("/api/products/available", response_model=dict)
async def get_available_products(
    email: str,
    type: Optional[str] = None,
    min_qty: Optional[float] = None,
    unit: Optional[str] = None
):
    """Get all available products (for traders)"""
    
    user = get_current_user_email(email)
    
    products = [p for p in products_db.values() if p["is_active"]]
    
    # Apply filters
    if type:
        products = [p for p in products if p["type"] == type]
    if min_qty:
        products = [p for p in products if p["available_qty"] >= min_qty]
    if unit:
        products = [p for p in products if p["unit"] == unit]
    
    # Add farmer info and current market price
    result = []
    for product in products:
        farmer = users_db.get(product["farmer_id"])
        
        # Get current market price
        current_price = None
        prices = [p for p in market_prices_db if p["product_type"] == product["type"]]
        if prices:
            prices.sort(key=lambda x: x["date"], reverse=True)
            current_price = prices[0]["price"]
        
        result.append({
            "_id": product["_id"],
            "farmer": {
                "_id": farmer["_id"],
                "name": farmer["name"],
                "location": f"{farmer['location']['city']}, {farmer['location']['state']}" if farmer.get("location") else "N/A"
            },
            "type": product["type"],
            "available_qty": product["available_qty"],
            "unit": product["unit"],
            "pending_contracts_count": len([
                c for c in contracts_db.values() 
                if c["product_id"] == product["_id"] and c["status"] == "PENDING"
            ]),
            "current_market_price": current_price
        })
    
    return {
        "success": True,
        "products": result
    }

# ============================================================================
# CONTRACT ENDPOINTS
# ============================================================================

@app.post("/api/farmer/create-contract", response_model=dict)
async def create_contract_by_farmer(request: CreateContractByFarmerRequest, email: str):
    """Farmer creates a contract"""
    
    user = get_current_user_email(email)
    
    if user["type"] != "farmer":
        raise HTTPException(status_code=403, detail="Only farmers can create farmer contracts")
    
    # Get product
    product = products_db.get(request.product_id)
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    
    if product["farmer_id"] != user["_id"]:
        raise HTTPException(status_code=403, detail="This product doesn't belong to you")
    
    # Validate quantity
    available_for_contract = product["available_qty"] - product["reserved_qty"]
    if request.qty > available_for_contract:
        raise HTTPException(
            status_code=400, 
            detail=f"Insufficient available quantity. Available: {available_for_contract} {product['unit']}"
        )
    
    # Create contract
    contract_id = generate_id()
    contract = {
        "_id": contract_id,
        "farmer_id": user["_id"],
        "trader_id": None,
        "product_id": request.product_id,
        "product_type": product["type"],
        "price_per_unit": request.price_per_unit,
        "qty": request.qty,
        "unit": request.unit.value,
        "total_value": request.price_per_unit * request.qty,
        "status": "PENDING",
        "created_by": "farmer",
        "created_at": datetime.now().isoformat(),
        "accepted_at": None,
        "accepted_by": None,
        "completed_at": None,
        "rejected_at": None,
        "cancelled_at": None,
        "expected_delivery_date": request.expected_delivery_date,
        "notes": request.notes
    }
    
    contracts_db[contract_id] = contract
    
    # Update product quantities
    product["reserved_qty"] += request.qty
    product["updated_at"] = datetime.now().isoformat()
    
    return {
        "success": True,
        "message": "Contract created successfully",
        "contract": contract
    }

@app.post("/api/trader/create-contract", response_model=dict)
async def create_contract_by_trader(request: CreateContractByTraderRequest, email: str):
    """Trader creates a contract"""
    
    user = get_current_user_email(email)
    
    if user["type"] != "trader":
        raise HTTPException(status_code=403, detail="Only traders can create trader contracts")
    
    # Get product
    product = products_db.get(request.product_id)
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    
    # Validate farmer
    if request.farmer_id != product["farmer_id"]:
        raise HTTPException(status_code=400, detail="Product doesn't belong to specified farmer")
    
    # Validate quantity
    if request.qty > product["available_qty"]:
        raise HTTPException(
            status_code=400,
            detail=f"Quantity exceeds available quantity. Available: {product['available_qty']} {product['unit']}"
        )
    
    # Create contract
    contract_id = generate_id()
    contract = {
        "_id": contract_id,
        "farmer_id": request.farmer_id,
        "trader_id": user["_id"],
        "product_id": request.product_id,
        "product_type": product["type"],
        "price_per_unit": request.price_per_unit,
        "qty": request.qty,
        "unit": request.unit.value,
        "total_value": request.price_per_unit * request.qty,
        "status": "PENDING",
        "created_by": "trader",
        "created_at": datetime.now().isoformat(),
        "accepted_at": None,
        "accepted_by": None,
        "completed_at": None,
        "rejected_at": None,
        "cancelled_at": None,
        "expected_delivery_date": request.expected_delivery_date,
        "notes": request.notes
    }
    
    contracts_db[contract_id] = contract
    
    # No quantity changes for trader-created contracts until farmer accepts
    
    return {
        "success": True,
        "message": "Contract created successfully",
        "contract": contract
    }

@app.post("/api/trader/accept-contract", response_model=dict)
async def trader_accept_contract(request: AcceptContractRequest, email: str):
    """Trader accepts a farmer's contract"""
    
    user = get_current_user_email(email)
    
    if user["type"] != "trader":
        raise HTTPException(status_code=403, detail="Only traders can accept farmer contracts")
    
    # Get contract
    contract = contracts_db.get(request.contract_id)
    if not contract:
        raise HTTPException(status_code=404, detail="Contract not found")
    
    if contract["created_by"] != "farmer":
        raise HTTPException(status_code=400, detail="This is not a farmer-created contract")
    
    if contract["status"] != "PENDING":
        raise HTTPException(status_code=400, detail="Contract is not pending")
    
    # Update contract
    contract["trader_id"] = user["_id"]
    contract["status"] = "ACTIVE"
    contract["accepted_at"] = datetime.now().isoformat()
    contract["accepted_by"] = user["_id"]
    
    # Update product quantities
    product = products_db[contract["product_id"]]
    product["reserved_qty"] -= contract["qty"]
    product["committed_qty"] += contract["qty"]
    product["available_qty"] -= contract["qty"]
    product["updated_at"] = datetime.now().isoformat()
    
    return {
        "success": True,
        "message": "Contract accepted successfully",
        "contract": contract
    }

@app.post("/api/farmer/accept-contract", response_model=dict)
async def farmer_accept_contract(request: AcceptContractRequest, email: str):
    """Farmer accepts a trader's contract"""
    
    user = get_current_user_email(email)
    
    if user["type"] != "farmer":
        raise HTTPException(status_code=403, detail="Only farmers can accept trader contracts")
    
    # Get contract
    contract = contracts_db.get(request.contract_id)
    if not contract:
        raise HTTPException(status_code=404, detail="Contract not found")
    
    if contract["created_by"] != "trader":
        raise HTTPException(status_code=400, detail="This is not a trader-created contract")
    
    if contract["status"] != "PENDING":
        raise HTTPException(status_code=400, detail="Contract is not pending")
    
    if contract["farmer_id"] != user["_id"]:
        raise HTTPException(status_code=403, detail="This contract is not for you")
    
    # Validate quantity at acceptance time
    product = products_db[contract["product_id"]]
    if contract["qty"] > product["available_qty"]:
        raise HTTPException(
            status_code=400,
            detail=f"Insufficient available quantity. Available: {product['available_qty']} {product['unit']}"
        )
    
    # Update contract
    contract["status"] = "ACTIVE"
    contract["accepted_at"] = datetime.now().isoformat()
    contract["accepted_by"] = user["_id"]
    
    # Update product quantities
    product["available_qty"] -= contract["qty"]
    product["committed_qty"] += contract["qty"]
    product["updated_at"] = datetime.now().isoformat()
    
    return {
        "success": True,
        "message": "Contract accepted successfully",
        "contract": contract
    }

@app.post("/api/reject-contract", response_model=dict)
async def reject_contract(request: RejectContractRequest, email: str):
    """Reject a pending contract"""
    
    user = get_current_user_email(email)
    
    # Get contract
    contract = contracts_db.get(request.contract_id)
    if not contract:
        raise HTTPException(status_code=404, detail="Contract not found")
    
    if contract["status"] != "PENDING":
        raise HTTPException(status_code=400, detail="Contract is not pending")
    
    # Check if user is the recipient (not creator)
    if contract["created_by"] == "farmer":
        # Trader should reject
        if user["type"] != "trader":
            raise HTTPException(status_code=403, detail="Only traders can reject farmer contracts")
    else:
        # Farmer should reject
        if user["type"] != "farmer" or contract["farmer_id"] != user["_id"]:
            raise HTTPException(status_code=403, detail="Only the specified farmer can reject this contract")
    
    # Update contract
    contract["status"] = "REJECTED"
    contract["rejected_at"] = datetime.now().isoformat()
    contract["rejected_by"] = user["_id"]
    contract["rejection_reason"] = request.rejection_reason
    
    # If farmer created the contract, return reserved quantity
    if contract["created_by"] == "farmer":
        product = products_db[contract["product_id"]]
        product["reserved_qty"] -= contract["qty"]
        product["updated_at"] = datetime.now().isoformat()
    
    return {
        "success": True,
        "message": "Contract rejected",
        "contract": contract
    }

@app.post("/api/cancel-contract", response_model=dict)
async def cancel_contract(request: CancelContractRequest, email: str):
    """Cancel own pending contract"""
    
    user = get_current_user_email(email)
    
    # Get contract
    contract = contracts_db.get(request.contract_id)
    if not contract:
        raise HTTPException(status_code=404, detail="Contract not found")
    
    if contract["status"] != "PENDING":
        raise HTTPException(status_code=400, detail="Contract is not pending")
    
    # Check if user is the creator
    if contract["created_by"] == "farmer":
        if user["type"] != "farmer" or contract["farmer_id"] != user["_id"]:
            raise HTTPException(status_code=403, detail="Only the contract creator can cancel it")
    else:
        if user["type"] != "trader" or contract["trader_id"] != user["_id"]:
            raise HTTPException(status_code=403, detail="Only the contract creator can cancel it")
    
    # Update contract
    contract["status"] = "CANCELLED"
    contract["cancelled_at"] = datetime.now().isoformat()
    
    # If farmer created the contract, return reserved quantity
    if contract["created_by"] == "farmer":
        product = products_db[contract["product_id"]]
        product["reserved_qty"] -= contract["qty"]
        product["updated_at"] = datetime.now().isoformat()
    
    return {
        "success": True,
        "message": "Contract cancelled",
        "contract": contract
    }

@app.post("/api/complete-contract", response_model=dict)
async def complete_contract(request: CompleteContractRequest, email: str):
    """Mark contract as completed"""
    
    user = get_current_user_email(email)
    
    # Get contract
    contract = contracts_db.get(request.contract_id)
    if not contract:
        raise HTTPException(status_code=404, detail="Contract not found")
    
    if contract["status"] != "ACTIVE":
        raise HTTPException(status_code=400, detail="Contract is not active")
    
    # Check if user is part of the contract
    if user["_id"] not in [contract["farmer_id"], contract["trader_id"]]:
        raise HTTPException(status_code=403, detail="You are not part of this contract")
    
    # Update contract
    contract["status"] = "COMPLETED"
    contract["completed_at"] = datetime.now().isoformat()
    contract["completed_by"] = request.completed_by.value
    
    # Update product quantities (actual delivery)
    product = products_db[contract["product_id"]]
    product["committed_qty"] -= contract["qty"]
    product["total_qty"] -= contract["qty"]
    product["updated_at"] = datetime.now().isoformat()
    
    return {
        "success": True,
        "message": "Contract marked as completed",
        "contract": contract
    }

@app.get("/api/contract/{contract_id}", response_model=dict)
async def get_contract_details(contract_id: str, email: str):
    """Get contract details"""
    
    user = get_current_user_email(email)
    
    contract = contracts_db.get(contract_id)
    if not contract:
        raise HTTPException(status_code=404, detail="Contract not found")
    
    # Get farmer and trader info
    farmer = users_db.get(contract["farmer_id"])
    trader = users_db.get(contract["trader_id"]) if contract["trader_id"] else None
    product = products_db.get(contract["product_id"])
    
    result = {
        **contract,
        "farmer": {
            "_id": farmer["_id"],
            "name": farmer["name"],
            "location": f"{farmer['location']['city']}, {farmer['location']['state']}" if farmer.get("location") else "N/A"
        } if farmer else None,
        "trader": {
            "_id": trader["_id"],
            "name": trader["name"]
        } if trader else None,
        "product": {
            "_id": product["_id"],
            "type": product["type"]
        } if product else None
    }
    
    return {
        "success": True,
        "contract": result
    }

@app.get("/api/farmer/active-contracts", response_model=dict)
async def get_farmer_active_contracts(email: str, farmer_id: Optional[str] = None):
    """Get farmer's active contracts"""
    
    user = get_current_user_email(email)
    
    target_farmer_id = farmer_id if farmer_id else user["_id"]
    
    contracts = [
        c for c in contracts_db.values() 
        if c["farmer_id"] == target_farmer_id and c["status"] == "ACTIVE"
    ]
    
    # Add trader info
    result = []
    for contract in contracts:
        trader = users_db.get(contract["trader_id"])
        result.append({
            **contract,
            "trader": {"name": trader["name"]} if trader else None
        })
    
    return {
        "success": True,
        "contracts": result
    }

@app.get("/api/trader/active-contracts", response_model=dict)
async def get_trader_active_contracts(email: str, trader_id: Optional[str] = None):
    """Get trader's active contracts"""
    
    user = get_current_user_email(email)
    
    target_trader_id = trader_id if trader_id else user["_id"]
    
    contracts = [
        c for c in contracts_db.values() 
        if c["trader_id"] == target_trader_id and c["status"] == "ACTIVE"
    ]
    
    # Add farmer info
    result = []
    for contract in contracts:
        farmer = users_db.get(contract["farmer_id"])
        result.append({
            **contract,
            "farmer": {
                "name": farmer["name"],
                "location": f"{farmer['location']['city']}, {farmer['location']['state']}" if farmer.get("location") else "N/A"
            } if farmer else None
        })
    
    return {
        "success": True,
        "contracts": result
    }

@app.get("/api/farmer/pending-contracts", response_model=dict)
async def get_farmer_pending_contracts(email: str, farmer_id: Optional[str] = None):
    """Get farmer's pending contracts"""
    
    user = get_current_user_email(email)
    
    target_farmer_id = farmer_id if farmer_id else user["_id"]
    
    contracts = [
        c for c in contracts_db.values() 
        if c["farmer_id"] == target_farmer_id and c["status"] == "PENDING"
    ]
    
    # Add trader info
    result = []
    for contract in contracts:
        trader = users_db.get(contract["trader_id"]) if contract["trader_id"] else None
        result.append({
            **contract,
            "trader": {"name": trader["name"]} if trader else None
        })
    
    return {
        "success": True,
        "contracts": result
    }

@app.get("/api/trader/pending-contracts", response_model=dict)
async def get_trader_pending_contracts(email: str, trader_id: Optional[str] = None):
    """Get trader's pending contracts"""
    
    user = get_current_user_email(email)
    
    target_trader_id = trader_id if trader_id else user["_id"]
    
    # Get contracts where trader needs to take action
    # (farmer-created contracts with no trader_id OR trader-created contracts by this trader)
    contracts = [
        c for c in contracts_db.values() 
        if c["status"] == "PENDING" and (
            (c["created_by"] == "farmer" and c["trader_id"] is None) or
            (c["created_by"] == "trader" and c["trader_id"] == target_trader_id)
        )
    ]
    
    # Add farmer info
    result = []
    for contract in contracts:
        farmer = users_db.get(contract["farmer_id"])
        result.append({
            **contract,
            "farmer": {
                "name": farmer["name"],
                "location": f"{farmer['location']['city']}, {farmer['location']['state']}" if farmer.get("location") else "N/A"
            } if farmer else None
        })
    
    return {
        "success": True,
        "contracts": result
    }

@app.get("/api/product/{product_id}/contracts", response_model=dict)
async def get_product_contracts(product_id: str, email: str, status: Optional[str] = None):
    """Get all contracts for a product"""
    
    user = get_current_user_email(email)
    
    contracts = [c for c in contracts_db.values() if c["product_id"] == product_id]
    
    if status:
        contracts = [c for c in contracts if c["status"] == status]
    
    # Add trader info
    result = []
    for contract in contracts:
        trader = users_db.get(contract["trader_id"]) if contract["trader_id"] else None
        result.append({
            **contract,
            "trader": {"name": trader["name"]} if trader else None
        })
    
    return {
        "success": True,
        "contracts": result
    }

# ============================================================================
# MARKET PRICE ENDPOINTS
# ============================================================================

@app.get("/api/market/current-prices", response_model=dict)
async def get_current_prices(email: str, type: Optional[str] = None):
    """Get current market prices"""
    
    user = get_current_user_email(email)
    
    # Get latest price for each product type
    product_types = ["Soybean", "Sunflower", "Groundnut", "Mustard", "Sesame"]
    if type:
        product_types = [type]
    
    result = []
    for product_type in product_types:
        prices = [p for p in market_prices_db if p["product_type"] == product_type]
        if prices:
            prices.sort(key=lambda x: x["date"], reverse=True)
            latest = prices[0]
            result.append({
                "product_type": latest["product_type"],
                "price": latest["price"],
                "unit": latest["unit"],
                "date": latest["date"],
                "source": latest["source"]
            })
    
    return {
        "success": True,
        "prices": result
    }

@app.get("/api/market/price-history", response_model=dict)
async def get_price_history(email: str, type: str, days: int = 30):
    """Get price history"""
    
    user = get_current_user_email(email)
    
    # Get prices for the specified product type
    prices = [p for p in market_prices_db if p["product_type"] == type]
    prices.sort(key=lambda x: x["date"], reverse=True)
    
    # Limit to specified days
    prices = prices[:days]
    prices.reverse()  # Chronological order
    
    history = [{"date": p["date"], "price": p["price"]} for p in prices]
    
    # Calculate statistics
    price_values = [p["price"] for p in prices]
    statistics = {
        "min": min(price_values) if price_values else 0,
        "max": max(price_values) if price_values else 0,
        "avg": sum(price_values) / len(price_values) if price_values else 0,
        "volatility": round(max(price_values) - min(price_values), 2) if price_values else 0
    }
    
    return {
        "success": True,
        "product_type": type,
        "history": history,
        "statistics": statistics
    }

# ============================================================================
# FORECAST ENDPOINTS
# ============================================================================

@app.get("/api/forecasts", response_model=dict)
async def get_forecasts(email: str, type: Optional[str] = None, days: int = 30):
    """Get price forecasts"""
    
    user = get_current_user_email(email)
    
    product_types = ["Soybean", "Sunflower", "Groundnut", "Mustard", "Sesame"]
    if type:
        product_types = [type]
    
    result = []
    for product_type in product_types:
        forecasts = [f for f in forecasts_db if f["product_type"] == product_type]
        forecasts.sort(key=lambda x: x["forecast_date"])
        
        # Limit to specified days
        forecasts = forecasts[:days]
        
        predictions = [
            {
                "date": f["forecast_date"],
                "predicted_price": f["predicted_price"],
                "confidence_lower": f["confidence_lower"],
                "confidence_upper": f["confidence_upper"]
            }
            for f in forecasts
        ]
        
        if forecasts:
            result.append({
                "product_type": product_type,
                "predictions": predictions,
                "model_version": forecasts[0]["model_version"],
                "generated_at": forecasts[0]["generated_at"]
            })
    
    return {
        "success": True,
        "forecasts": result
    }

# ============================================================================
# DASHBOARD ENDPOINTS
# ============================================================================

@app.get("/api/farmer/dashboard-summary", response_model=dict)
async def get_farmer_dashboard_summary(email: str, farmer_id: Optional[str] = None):
    """Get farmer dashboard summary"""
    
    user = get_current_user_email(email)
    
    target_farmer_id = farmer_id if farmer_id else user["_id"]
    
    # Get products
    products = [p for p in products_db.values() if p["farmer_id"] == target_farmer_id]
    
    # Get current prices for inventory valuation
    product_prices = {}
    for p in market_prices_db:
        if p["product_type"] not in product_prices:
            product_prices[p["product_type"]] = []
        product_prices[p["product_type"]].append(p)
    
    total_inventory_value = 0
    products_by_type = []
    for product in products:
        # Get latest price
        prices = product_prices.get(product["type"], [])
        if prices:
            prices.sort(key=lambda x: x["date"], reverse=True)
            current_price = prices[0]["price"]
            total_inventory_value += product["total_qty"] * current_price
        
        products_by_type.append({
            "type": product["type"],
            "total_qty": product["total_qty"],
            "available_qty": product["available_qty"],
            "unit": product["unit"]
        })
    
    # Get contracts
    all_contracts = [c for c in contracts_db.values() if c["farmer_id"] == target_farmer_id]
    active_contracts = [c for c in all_contracts if c["status"] == "ACTIVE"]
    pending_contracts = [c for c in all_contracts if c["status"] == "PENDING"]
    completed_contracts = [c for c in all_contracts if c["status"] == "COMPLETED"]
    
    total_active_value = sum(c["total_value"] for c in active_contracts)
    total_pending_value = sum(c["total_value"] for c in pending_contracts)
    
    # Recent activity
    recent = sorted(all_contracts, key=lambda x: x["created_at"], reverse=True)[:5]
    recent_activity = []
    for contract in recent:
        trader = users_db.get(contract["trader_id"]) if contract["trader_id"] else None
        
        if contract["status"] == "ACTIVE" and contract.get("accepted_at"):
            activity_type = "contract_accepted"
            timestamp = contract["accepted_at"]
        elif contract["status"] == "COMPLETED":
            activity_type = "contract_completed"
            timestamp = contract["completed_at"]
        else:
            activity_type = "contract_created"
            timestamp = contract["created_at"]
        
        recent_activity.append({
            "type": activity_type,
            "contract_id": contract["_id"],
            "trader_name": trader["name"] if trader else "Pending",
            "amount": contract["total_value"],
            "timestamp": timestamp
        })
    
    return {
        "success": True,
        "summary": {
            "products": {
                "total_count": len(products),
                "total_inventory_value": round(total_inventory_value, 2),
                "by_type": products_by_type
            },
            "contracts": {
                "active_count": len(active_contracts),
                "pending_count": len(pending_contracts),
                "completed_count": len(completed_contracts),
                "total_active_value": round(total_active_value, 2),
                "total_pending_value": round(total_pending_value, 2)
            },
            "recent_activity": recent_activity
        }
    }

@app.get("/api/trader/dashboard-summary", response_model=dict)
async def get_trader_dashboard_summary(email: str, trader_id: Optional[str] = None):
    """Get trader dashboard summary"""
    
    user = get_current_user_email(email)
    
    target_trader_id = trader_id if trader_id else user["_id"]
    
    # Get contracts
    all_contracts = [c for c in contracts_db.values() if c["trader_id"] == target_trader_id]
    active_contracts = [c for c in all_contracts if c["status"] == "ACTIVE"]
    pending_contracts = [c for c in all_contracts if c["status"] == "PENDING"]
    completed_contracts = [c for c in all_contracts if c["status"] == "COMPLETED"]
    
    total_active_value = sum(c["total_value"] for c in active_contracts)
    total_pending_value = sum(c["total_value"] for c in pending_contracts)
    
    # By product type
    by_product = {}
    for contract in active_contracts:
        product_type = contract["product_type"]
        if product_type not in by_product:
            by_product[product_type] = {
                "type": product_type,
                "active_contracts": 0,
                "total_qty": 0,
                "total_value": 0
            }
        by_product[product_type]["active_contracts"] += 1
        by_product[product_type]["total_qty"] += contract["qty"]
        by_product[product_type]["total_value"] += contract["total_value"]
    
    by_product_list = []
    for product_type, data in by_product.items():
        data["avg_price"] = round(data["total_value"] / data["total_qty"], 2) if data["total_qty"] > 0 else 0
        by_product_list.append(data)
    
    # Recent activity
    recent = sorted(all_contracts, key=lambda x: x["created_at"], reverse=True)[:5]
    recent_activity = []
    for contract in recent:
        farmer = users_db.get(contract["farmer_id"])
        
        if contract["status"] == "ACTIVE" and contract.get("accepted_at"):
            activity_type = "contract_accepted"
            timestamp = contract["accepted_at"]
        elif contract["status"] == "COMPLETED":
            activity_type = "contract_completed"
            timestamp = contract["completed_at"]
        else:
            activity_type = "contract_created"
            timestamp = contract["created_at"]
        
        recent_activity.append({
            "type": activity_type,
            "contract_id": contract["_id"],
            "farmer_name": farmer["name"] if farmer else "Unknown",
            "amount": contract["total_value"],
            "timestamp": timestamp
        })
    
    return {
        "success": True,
        "summary": {
            "contracts": {
                "active_count": len(active_contracts),
                "pending_count": len(pending_contracts),
                "completed_count": len(completed_contracts),
                "total_active_value": round(total_active_value, 2),
                "total_pending_value": round(total_pending_value, 2)
            },
            "by_product": by_product_list,
            "recent_activity": recent_activity
        }
    }

# ============================================================================
# ROOT ENDPOINT
# ============================================================================

@app.get("/")
async def root():
    return {
        "message": "Hedging Platform API",
        "version": "1.0.0",
        "status": "running",
        "docs": "/docs"
    }

@app.get("/api/health")
async def health_check():
    return {
        "status": "healthy",
        "users": len(users_db),
        "products": len(products_db),
        "contracts": len(contracts_db),
        "market_prices": len(market_prices_db),
        "forecasts": len(forecasts_db)
    }

# ============================================================================
# RUN SERVER
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)