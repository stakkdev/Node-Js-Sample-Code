const Crypto = require('crypto');

module.exports = function Cart(cart) {
    this.items = cart['Item_details'] || {};
    this.comboItems = cart['Combo_meals'] || {};
    this.totalItems = cart.totalItems || 0;
    this.totalPrice = cart.Sub_total || 0;
    
    this.addItem = function(item) {
        var bookingId = Math.round(new Date().getTime());
        this.items[bookingId] = item;
        this.totalItems++;
        this.totalPrice += parseFloat(item.Item_rate);
        item['Condiments'].forEach((condiment) => {
            this.totalPrice = this.totalPrice + parseFloat(condiment['Item_rate']);
        })
        return bookingId;
    };

    this.addComboItem = function(item) {
        var bookingId = Math.round(new Date().getTime());
        
        this.totalItems++;
        this.totalPrice += parseFloat(item['totalComboPrice']);
        delete item['totalComboPrice'];
        this.comboItems[bookingId] = item;
        return bookingId;
    };

    this.updateComboItem = function(item, id) {

        // Quantity will remain same
        var quantity = this.comboItems[id]['ComboMealMenuItem']['Item_qty'];

        // Remove old combo meal amount from cart 
        this.totalPrice = this.totalPrice - (quantity * parseFloat(this.comboItems[id]['ComboMealMenuItem']['Item_rate']));
        this.comboItems[id]['ComboMealMenuItem']['Condiments'].forEach((condiment) => {
            this.totalPrice = this.totalPrice - (condiment['Item_qty'] * parseFloat(condiment['Item_rate']));
        });
        this.comboItems[id]['ComboMealMainItem']['Condiments'].forEach((condiment) => {
            this.totalPrice = this.totalPrice - (condiment['Item_qty'] * parseFloat(condiment['Item_rate']));
        })
        this.comboItems[id]['SideItems'].forEach((sideItem) => {
            this.totalPrice = this.totalPrice - (sideItem.Item_qty * parseFloat(sideItem.Item_rate));
            sideItem.Condiments.forEach((condiment) => {
                this.totalPrice = this.totalPrice - (condiment.Item_qty * parseFloat(condiment.Item_rate));
            })
        });

        // Remove new combo meal amount from cart 
        this.totalPrice += (quantity * parseFloat(item['totalComboPrice']));
        delete item['totalComboPrice'];
        
        this.comboItems[id] = item;
        return id;
    };

    this.incQuantity = function(id) {
        if(this.items[id]) {
            this.totalItems++;
            this.totalPrice = this.totalPrice + parseFloat(this.items[id]['Item_rate']);
            this.items[id]['Item_qty']++;
            this.items[id]['Condiments'].forEach((condiment) => {
                condiment.Item_qty++;
                this.totalPrice = this.totalPrice + parseFloat(condiment['Item_rate']);
            });
            return this.items[id]['Item_code'];
        }
        if(this.comboItems[id]) {
            this.totalItems++;
            this.totalPrice = this.totalPrice + parseFloat(this.comboItems[id]['ComboMealMenuItem']['Item_rate']);
            this.comboItems[id]['ComboMealMenuItem']['Item_qty']++;
            this.comboItems[id]['ComboMealMenuItem']['Condiments'].forEach((condiment) => {
                condiment.Item_qty++;
                this.totalPrice = this.totalPrice + parseFloat(condiment['Item_rate']);
            });
            this.comboItems[id]['ComboMealMainItem']['Item_qty']++;
            this.comboItems[id]['ComboMealMainItem']['Condiments'].forEach((condiment) => {
                condiment.Item_qty++;
                this.totalPrice = this.totalPrice + parseFloat(condiment['Item_rate']);
            })
            this.comboItems[id]['SideItems'].forEach((sideItem) => {
                sideItem.Item_qty++;
                this.totalPrice = this.totalPrice + parseFloat(sideItem.Item_rate);
                sideItem.Condiments.forEach((condiment) => {
                    condiment.Item_qty++;
                    this.totalPrice = this.totalPrice + parseFloat(condiment.Item_rate);
                })
            });
            return this.comboItems[id]['ComboMealMenuItem']['Item_code'];
        }
    };
    
    this.decQuantity = function(id) {
        if(this.items[id]) {
            if(this.items[id]['Item_qty'] == 1) {
                this.removeItem(id);
            } else {
                this.totalItems--;
                this.totalPrice = this.totalPrice - parseFloat(this.items[id]['Item_rate']);
                this.items[id]['Item_qty']--;
                this.items[id]['Condiments'].forEach((condiment) => {
                    condiment.Item_qty--;
                    this.totalPrice = this.totalPrice - parseFloat(condiment['Item_rate']);
                })
            }
        }
        if(this.comboItems[id]) {
            if(this.comboItems[id]['ComboMealMenuItem']['Item_qty'] == 1) {
                this.removeItem(id);
            } else {
                this.totalItems--;
                this.totalPrice = this.totalPrice - parseFloat(this.comboItems[id]['ComboMealMenuItem']['Item_rate']);
                this.comboItems[id]['ComboMealMenuItem']['Item_qty']--;
                this.comboItems[id]['ComboMealMenuItem']['Condiments'].forEach((condiment) => {
                    condiment.Item_qty--;
                    this.totalPrice = this.totalPrice - parseFloat(condiment['Item_rate']);
                });
                this.comboItems[id]['ComboMealMainItem']['Item_qty']--;
                this.comboItems[id]['ComboMealMainItem']['Condiments'].forEach((condiment) => {
                    condiment.Item_qty--;
                    this.totalPrice = this.totalPrice - parseFloat(condiment['Item_rate']);
                })
                this.comboItems[id]['SideItems'].forEach((sideItem) => {
                    this.totalPrice = this.totalPrice - parseFloat(sideItem.Item_rate);
                    sideItem.Condiments.forEach((condiment) => {
                        condiment.Item_qty--;
                        this.totalPrice = this.totalPrice - parseFloat(condiment.Item_rate);
                    })
                });
            }
        }
    };

    this.removeItem = function(id) {
        if(this.items[id]) {
            this.totalItems = this.totalItems - this.items[id]['Item_qty'];
            this.totalPrice = this.totalPrice - (this.items[id]['Item_qty'] * parseFloat(this.items[id]['Item_rate']));

            this.items[id]['Condiments'].forEach((condiment) => {
                this.totalPrice = this.totalPrice - (condiment['Item_qty'] * parseFloat(condiment['Item_rate']));
            });
            delete this.items[id];
        } else if (this.comboItems[id]) {
            this.totalItems = this.totalItems - this.comboItems[id]['ComboMealMenuItem']['Item_qty'];

            this.totalPrice = this.totalPrice - (this.comboItems[id]['ComboMealMenuItem']['Item_qty'] * parseFloat(this.comboItems[id]['ComboMealMenuItem']['Item_rate']));
            this.comboItems[id]['ComboMealMenuItem']['Condiments'].forEach((condiment) => {
                this.totalPrice = this.totalPrice - parseFloat(condiment['Item_rate']);
            });

            this.totalPrice = this.totalPrice - (this.comboItems[id]['ComboMealMainItem']['Item_qty'] * parseFloat(this.comboItems[id]['ComboMealMainItem']['Item_rate']));
            this.comboItems[id]['ComboMealMainItem']['Condiments'].forEach((condiment) => {
                this.totalPrice = this.totalPrice - parseFloat(condiment['Item_rate']);
            })
            this.comboItems[id]['SideItems'].forEach((sideItem) => {
                this.totalPrice = this.totalPrice - parseFloat(sideItem.Item_rate);
                sideItem.Condiments.forEach((condiment) => {
                    this.totalPrice = this.totalPrice - parseFloat(condiment.Item_rate);
                })
            });
            delete this.comboItems[id];
        }
    };
    
    this.updateCondiments = function(id, condiments) {
        if(this.items[id]) {
            var instruction = {};
            this.items[id]['Condiments'].forEach((condiment) => {
                this.totalPrice = this.totalPrice - (condiment['Item_qty'] * parseFloat(condiment['Item_rate']));
                if(condiment['Item_ref'] != "") {
                    instruction = condiment;
                }
            });
            this.items[id]['Condiments'] = [];
            condiments.forEach((cond) => {
                this.items[id]['Condiments'].push({
                    "Item_code": cond['Item_code'],
                    "Item_qty": this.items[id]['Item_qty'],
                    "Item_rate": cond['Item_rate'],
                    "Item_ref": ""
                });
                this.totalPrice = this.totalPrice + (this.items[id]['Item_qty'] * parseFloat(cond['Item_rate']));
            })
            if(instruction.Item_ref) {
                this.items[id]['Condiments'].push(instruction);
            }
        }
    };
    this.allItems = function() {
        var allItems = 0;
        Object.values(this.items).forEach((item) => {
            allItems += item['Item_qty'];
        })
        return this.totalItems = allItems;
    }
};
