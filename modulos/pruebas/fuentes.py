import pyfiglet

# Obtener todas las fuentes disponibles
fuentes = pyfiglet.FigletFont.getFonts()

# Mostrar las fuentes disponibles una por una
for fuente in fuentes:
    # Generar arte ASCII con la fuente seleccionada
    ascii_art = pyfiglet.figlet_format("PADRON NOMINAL", font=fuente)
    
    # Mostrar el resultado
    print(f"\n--- Arte con fuente {fuente} ---")
    print(ascii_art)
    
    # Esperar que el usuario presione Enter para continuar
    input("Presiona Enter para ver la siguiente fuente...")